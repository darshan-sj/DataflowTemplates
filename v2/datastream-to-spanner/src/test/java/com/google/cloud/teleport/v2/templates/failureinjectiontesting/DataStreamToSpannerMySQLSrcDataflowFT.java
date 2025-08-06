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
package com.google.cloud.teleport.v2.templates.failureinjectiontesting;

import static com.google.cloud.teleport.v2.templates.failureinjectiontesting.utils.MySQLSrcDataProvider.AUTHORS_TABLE;
import static com.google.cloud.teleport.v2.templates.failureinjectiontesting.utils.MySQLSrcDataProvider.BOOKS_TABLE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.failureinjectiontesting.utils.DataflowFailureInjector;
import com.google.cloud.teleport.v2.templates.failureinjectiontesting.utils.MySQLSrcDataProvider;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.logging.LoggingClient;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A failure injection test for the DataStream to Spanner template. This test simulates a Dataflow
 * worker failure scenario to ensure the pipeline remains resilient and processes all data without
 * loss.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerMySQLSrcDataflowFT extends DataStreamToSpannerFTBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerMySQLSrcDataflowFT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerFailureInjectionTesting/spanner-schema.sql";

  public static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  private static CloudSqlResourceManager sourceDBResourceManager;

  private static HashSet<DataStreamToSpannerMySQLSrcDataflowFT> testInstances = new HashSet<>();
  private JDBCSource sourceConnectionProfile;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerMySQLSrcDataflowFT.class) {
      testInstances.add(this);
      if (pubsubResourceManager == null) {
        // create Spanner Resources
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

        // create Source Resources
        sourceDBResourceManager =
            MySQLSrcDataProvider.createSourceResourceManagerWithSchema(testName);
        sourceConnectionProfile =
            createMySQLSourceConnectionProfile(
                sourceDBResourceManager, Arrays.asList(AUTHORS_TABLE, BOOKS_TABLE));

        // create and upload GCS Resources
        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();

        // create pubsub manager
        pubsubResourceManager = setUpPubSubResourceManager();
      }
    }
  }

  /**
   * Cleanup all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerMySQLSrcDataflowFT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, gcsResourceManager, pubsubResourceManager, sourceDBResourceManager);
  }

  @Test
  public void dataflowWorkerFailureTest()
      throws IOException, ExecutionException, InterruptedException {

    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testName);

    // launch forward migration template
    LaunchInfo jobInfo =
        launchFwdDataflowJob(
            spannerResourceManager,
            gcsResourceManager,
            pubsubResourceManager,
            flexTemplateBuilder,
            sourceConnectionProfile);

    // Wait for Forward migration job to be in running state
    assertThatPipeline(jobInfo).isRunning();

    // Wave of inserts
    MySQLSrcDataProvider.writeRowsInSourceDB(1, 10000, sourceDBResourceManager);

    // Wait for at least one row to appear in spanner
    ConditionCheck conditionCheck =
        SpannerRowsCheck.builder(spannerResourceManager, AUTHORS_TABLE).setMinRows(1).build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), conditionCheck);
    assertThatResult(result).meetsConditions();

    // Insert more data before killing the dataflow workers
    MySQLSrcDataProvider.writeRowsInSourceDB(10001, 20000, sourceDBResourceManager);

    // Kill all the workers of the dataflow job
    DataflowFailureInjector.abruptlyKillWorkers(jobInfo.projectId(), jobInfo.jobId());

    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, AUTHORS_TABLE)
                        .setMinRows(20000)
                        .setMaxRows(20000)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, BOOKS_TABLE)
                        .setMinRows(20000)
                        .setMaxRows(20000)
                        .build()))
            .build();

    result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void dataflowWorkerQuotaTest() throws IOException, InterruptedException {

    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testName).withRegion("us-west8");

    Map<String, String> params = Map.of("workerMachineType", "n2-standard-2");

    Map<String, Object> environmentVars = Map.of("workerRegion", "us-west8");

    try {
      LaunchInfo jobInfo =
          launchFwdDataflowJob(
              spannerResourceManager,
              gcsResourceManager,
              pubsubResourceManager,
              flexTemplateBuilder,
              sourceConnectionProfile,
              params,
              environmentVars);
      fail("Expected launch job to fail but it succeeded");
    } catch (RuntimeException e) {
      String jobId = extractJobIdFromError(e.getMessage());
      assertNotNull(jobId);
      LoggingClient loggingClient =
          LoggingClient.builder(credentials).setProjectId(PROJECT).build();
      String filter =
          String.format(
              " \"Workflow failed. Causes: Project %s has insufficient resource(s) to execute this workflow\" ",
              PROJECT);
      Instant startTime = Instant.now();
      Instant endTime = startTime.plus(Duration.ofMinutes(15L));
      Boolean errorLogFound = false;
      while (!errorLogFound && Instant.now().isBefore(endTime)) {
        List<Payload> logs = loggingClient.readJobLogs(jobId, filter, Severity.ERROR, 2);
        if (logs.size() > 0) {
          errorLogFound = true;
        }
        Thread.sleep(15 * 1000);
      }
      assertTrue("Could not find Expected dataflow job log: Insufficient resources", errorLogFound);
    }
  }

  private String extractJobIdFromError(String message) {
    Pattern pattern =
        Pattern.compile(
            "https://console.cloud.google.com/dataflow/jobs/([^/]+)/([^/?]+)(\\?project=([^&]+))?");
    Matcher matcher = pattern.matcher(message);
    if (matcher.find()) {
      // Group 2 contains the jobId
      return matcher.group(2);
    } else {
      return null;
    }
  }
}
