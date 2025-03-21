/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.spanner.spannerio.changestreams.action;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.spanner.spannerio.changestreams.dofn.ReadChangeStreamPartitionDoFn;
import com.google.cloud.teleport.spanner.spannerio.changestreams.estimator.ThroughputEstimator;
import com.google.cloud.teleport.spanner.spannerio.changestreams.model.DataChangeRecord;
import com.google.cloud.teleport.spanner.spannerio.changestreams.model.PartitionMetadata;
import com.google.cloud.teleport.spanner.spannerio.changestreams.restriction.RestrictionInterrupter;
import com.google.cloud.teleport.spanner.spannerio.changestreams.restriction.TimestampRange;
import java.util.Optional;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is part of the process for {@link ReadChangeStreamPartitionDoFn} SDF. It is
 * responsible for processing {@link DataChangeRecord}s. The records will simply be emitted to the
 * received output receiver.
 */
public class DataChangeRecordAction {
  private static final Logger LOG = LoggerFactory.getLogger(DataChangeRecordAction.class);
  private final ThroughputEstimator<DataChangeRecord> throughputEstimator;

  /**
   * @param throughputEstimator an estimator to calculate local throughput of this action.
   */
  public DataChangeRecordAction(ThroughputEstimator<DataChangeRecord> throughputEstimator) {
    this.throughputEstimator = throughputEstimator;
  }

  /**
   * This is the main processing function for a {@link DataChangeRecord}. It returns an {@link
   * Optional} of {@link ProcessContinuation} to indicate if the calling function should stop or
   * not. If the {@link Optional} returned is empty, it means that the calling function can continue
   * with the processing. If an {@link Optional} of {@link ProcessContinuation#stop()} is returned,
   * it means that this function was unable to claim the timestamp of the {@link DataChangeRecord},
   * so the caller should stop.
   *
   * <p>When processing the {@link DataChangeRecord} the following procedure is applied:
   *
   * <ol>
   *   <li>We try to cliam the data change record commit timestamp. If it is not possible, we stop
   *       here and return.
   *   <li>We emit the data change record through the {@link OutputReceiver}.
   *   <li>We update the watermark to the data change record commit timestamp.
   * </ol>
   *
   * @param partition the current partition being processed
   * @param record the change stream data record received
   * @param tracker the restriction tracker of the {@link ReadChangeStreamPartitionDoFn} SDF
   * @param interrupter the restriction interrupter suggesting early termination of the processing
   * @param outputReceiver the output receiver of the {@link ReadChangeStreamPartitionDoFn} SDF
   * @param watermarkEstimator the watermark estimator of the {@link ReadChangeStreamPartitionDoFn}
   *     SDF
   * @return {@link Optional#empty()} if the caller can continue processing more records. A non
   *     empty {@link Optional} with {@link ProcessContinuation#stop()} if this function was unable
   *     to claim the {@link DataChangeRecord} timestamp. A non empty {@link Optional} with {@link
   *     ProcessContinuation#resume()} if this function should commit what has already been
   *     processed and resume.
   */
  @VisibleForTesting
  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      DataChangeRecord record,
      RestrictionTracker<TimestampRange, Timestamp> tracker,
      RestrictionInterrupter<Timestamp> interrupter,
      OutputReceiver<DataChangeRecord> outputReceiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    final String token = partition.getPartitionToken();
    LOG.debug("[{}] Processing data record {}", token, record.getCommitTimestamp());

    final Timestamp commitTimestamp = record.getCommitTimestamp();
    final Instant commitInstant = new Instant(commitTimestamp.toSqlTimestamp().getTime());
    if (interrupter.tryInterrupt(commitTimestamp)) {
      LOG.debug(
          "[{}] Soft deadline reached with data change record at {}, rescheduling",
          token,
          commitTimestamp);
      return Optional.of(ProcessContinuation.resume());
    }
    if (!tracker.tryClaim(commitTimestamp)) {
      LOG.debug("[{}] Could not claim queryChangeStream({}), stopping", token, commitTimestamp);
      return Optional.of(ProcessContinuation.stop());
    }
    outputReceiver.outputWithTimestamp(record, commitInstant);
    watermarkEstimator.setWatermark(commitInstant);

    throughputEstimator.update(Timestamp.now(), record);

    LOG.debug("[{}] Data record action completed successfully", token);
    return Optional.empty();
  }
}
