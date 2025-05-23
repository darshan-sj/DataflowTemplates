/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateCreationParameter;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.spanner.ImportPipeline.Options;
import com.google.cloud.teleport.spanner.spannerio.SpannerConfig;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Avro to Cloud Spanner Import pipeline.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_Avro_to_Cloud_Spanner.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "GCS_Avro_to_Cloud_Spanner",
    category = TemplateCategory.BATCH,
    displayName = "Avro Files on Cloud Storage to Cloud Spanner",
    description =
        "The Cloud Storage Avro files to Cloud Spanner template is a batch pipeline that reads Avro files exported from "
            + "Cloud Spanner stored in Cloud Storage and imports them to a Cloud Spanner database.",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/avro-to-cloud-spanner",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The target Cloud Spanner database must exist and must be empty.",
      "You must have read permissions for the Cloud Storage bucket and write permissions for the target Cloud Spanner database.",
      "The input Cloud Storage path must exist, and it must include a <a href=\"https://cloud.google.com/spanner/docs/import-non-spanner#create-export-json\">spanner-export.json</a> file that contains a JSON description of files to import."
    })
public class ImportPipeline {

  /** Options for {@link ImportPipeline}. */
  public interface Options extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        groupName = "Target",
        regexes = {"^[a-z0-9\\-]+$"},
        description = "Cloud Spanner instance ID",
        helpText = "The instance ID of the Spanner database.")
    ValueProvider<String> getInstanceId();

    void setInstanceId(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Target",
        regexes = {"^[a-z_0-9\\-]+$"},
        description = "Cloud Spanner database ID",
        helpText = "The database ID of the Spanner database.")
    ValueProvider<String> getDatabaseId();

    void setDatabaseId(ValueProvider<String> value);

    @TemplateParameter.GcsReadFolder(
        order = 3,
        groupName = "Source",
        description = "Cloud storage input directory",
        helpText = "The Cloud Storage path where the Avro files are imported from.")
    ValueProvider<String> getInputDir();

    void setInputDir(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Target",
        optional = true,
        description = "Cloud Spanner Endpoint to call",
        helpText = "The Cloud Spanner endpoint to call in the template. Only used for testing.",
        example = "https://batch-spanner.googleapis.com")
    @Default.String("https://batch-spanner.googleapis.com")
    ValueProvider<String> getSpannerHost();

    void setSpannerHost(ValueProvider<String> value);

    @TemplateParameter.Boolean(
        order = 5,
        optional = true,
        description = "Wait for Indexes",
        helpText =
            "If `true`, the pipeline waits for indexes to be created. If `false`, the job might complete while indexes are still being created in the background. The default value is `false`.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getWaitForIndexes();

    void setWaitForIndexes(ValueProvider<Boolean> value);

    @TemplateParameter.Boolean(
        order = 6,
        optional = true,
        description = "Wait for Foreign Keys",
        helpText =
            "If `true`, the pipeline waits for foreign keys to be created. If `false`, the job might complete while foreign keys are still being created in the background. The default value is `false`.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getWaitForForeignKeys();

    void setWaitForForeignKeys(ValueProvider<Boolean> value);

    @TemplateParameter.Boolean(
        order = 7,
        optional = true,
        description = "Wait for Change Streams",
        helpText =
            "If `true`, the pipeline waits for change streams to be created. If `false`, the job might complete while change streams are still being created in the background. The default value is `true`.")
    @Default.Boolean(true)
    ValueProvider<Boolean> getWaitForChangeStreams();

    void setWaitForChangeStreams(ValueProvider<Boolean> value);

    @TemplateParameter.Boolean(
        order = 7,
        optional = true,
        description = "Wait for Sequences",
        helpText =
            "By default, the import pipeline is blocked on sequence creation. If `false`, the import pipeline might"
                + " complete with sequences still being created in the background.")
    @Default.Boolean(true)
    ValueProvider<Boolean> getWaitForSequences();

    void setWaitForSequences(ValueProvider<Boolean> value);

    @TemplateParameter.Boolean(
        order = 8,
        optional = true,
        description = "Create Indexes early",
        helpText =
            "Specifies whether early index creation is enabled. If the template runs a large number of DDL statements, it's more efficient to create indexes before loading data. Therefore, the default behavior is to create the indexes first when the number of DDL statements exceeds a threshold. To disable this feature, set `earlyIndexCreateFlag` to `false`. The default value is `true`.")
    @Default.Boolean(true)
    ValueProvider<Boolean> getEarlyIndexCreateFlag();

    void setEarlyIndexCreateFlag(ValueProvider<Boolean> value);

    @TemplateCreationParameter(value = "false")
    @Description("If true, wait for job finish")
    @Default.Boolean(true)
    boolean getWaitUntilFinish();

    @TemplateParameter.ProjectId(
        order = 9,
        groupName = "Target",
        optional = true,
        description = "Cloud Spanner Project Id",
        helpText =
            "The ID of the Google Cloud project that contains the Spanner database. If not set, the default Google Cloud project is used.")
    ValueProvider<String> getSpannerProjectId();

    void setSpannerProjectId(ValueProvider<String> value);

    void setWaitUntilFinish(boolean value);

    @TemplateParameter.Integer(
        order = 10,
        optional = true,
        description = "DDL Creation timeout in minutes",
        helpText =
            "The timeout in minutes for DDL statements performed by the template. The default value is 30 minutes.")
    @Default.Integer(30)
    ValueProvider<Integer> getDdlCreationTimeoutInMinutes();

    void setDdlCreationTimeoutInMinutes(ValueProvider<Integer> value);

    @TemplateParameter.Enum(
        order = 11,
        groupName = "Target",
        enumOptions = {
          @TemplateEnumOption("LOW"),
          @TemplateEnumOption("MEDIUM"),
          @TemplateEnumOption("HIGH")
        },
        optional = true,
        description = "Priority for Spanner RPC invocations",
        helpText =
            "The request priority for Spanner calls. Possible values are `HIGH`, `MEDIUM`, and `LOW`. The default value is `MEDIUM`.")
    ValueProvider<RpcPriority> getSpannerPriority();

    void setSpannerPriority(ValueProvider<RpcPriority> value);
  }

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            // Temporary fix explicitly setting SpannerConfig.projectId to the default project
            // if spannerProjectId is not provided as a parameter. Required as of Beam 2.38,
            // which no longer accepts null label values on metrics, and SpannerIO#setup() has
            // a bug resulting in the label value being set to the original parameter value,
            // with no fallback to the default project.
            // TODO: remove NestedValueProvider when this is fixed in Beam.
            .withProjectId(
                NestedValueProvider.of(
                    options.getSpannerProjectId(),
                    (SerializableFunction<String, String>)
                        input -> input != null ? input : SpannerOptions.getDefaultProjectId()))
            .withHost(options.getSpannerHost())
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId())
            .withRpcPriority(options.getSpannerPriority());

    p.apply(
        new ImportTransform(
            spannerConfig,
            options.getInputDir(),
            options.getWaitForIndexes(),
            options.getWaitForForeignKeys(),
            options.getWaitForChangeStreams(),
            options.getWaitForSequences(),
            options.getEarlyIndexCreateFlag(),
            options.getDdlCreationTimeoutInMinutes()));

    PipelineResult result = p.run();

    if (options.getWaitUntilFinish()
        &&
        /* Only if template location is null, there is a dataflow job to wait for. Else it's
         * template generation which doesn't start a dataflow job.
         */
        options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }
}
