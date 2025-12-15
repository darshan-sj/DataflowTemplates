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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests all data types
 * migration with custom transformations, bulk failure injection, and live retry.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLAllDataTypesCustomTransformationsBulkAndLiveFT extends SourceDbToSpannerFTBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(MySQLAllDataTypesCustomTransformationsBulkAndLiveFT.class);

  private static final String MYSQL_DDL_RESOURCE =
      "MySQLAllDataTypesCustomTransformationsBulkAndLiveFT/mysql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "MySQLAllDataTypesCustomTransformationsBulkAndLiveFT/spanner-schema.sql";
  private static final String TABLE_NAME = "AllDataTypes";

  private static PipelineLauncher.LaunchInfo bulkJobInfo;
  private static PipelineLauncher.LaunchInfo retryLiveJobInfo;

  public static CloudMySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static String bulkErrorFolderFullPath;

  @Before
  public void setUp() throws Exception {
    // create Spanner Resources
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

    // create MySQL Resources
    mySQLResourceManager = setUpMySQLResourceManager();
    loadSQLFileResource(mySQLResourceManager, MYSQL_DDL_RESOURCE);

    // create and upload GCS Resources
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();

    // Upload Custom Transformation Jar
    createAndUploadJarToGcs("CustomTransformationAllTypes");

    // Insert Data
    insertData();

    // create pubsub manager
    pubsubResourceManager = setUpPubSubResourceManager();

    bulkErrorFolderFullPath = getGcsPath("output", gcsResourceManager);

    // Define Custom Transformation with Exception (Bad)
    CustomTransformation customTransformationBad =
        CustomTransformation.builder(
                "customTransformation.jar", "com.custom.CustomTransformationAllTypesWithException")
            .build();

    // launch bulk migration
    bulkJobInfo =
        launchBulkDataflowJob(
            getClass().getSimpleName(),
            spannerResourceManager,
            gcsResourceManager,
            mySQLResourceManager,
            customTransformationBad);
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager, spannerResourceManager, gcsResourceManager, pubsubResourceManager);
  }

  @Test
  public void testAllDataTypesCustomTransformationsBulkAndLive()
      throws IOException, InterruptedException {
    // Wait for Bulk migration job to be in running state
    assertThatPipeline(bulkJobInfo).isRunning();

    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(bulkJobInfo, Duration.ofMinutes(30)));
    assertThatResult(result).isLaunchFinished();

    // Verify DLQ has 1 event (the "fail_me" row)
    // Total events = successfully processed events (2) + errors in output folder (1)
    ConditionCheck conditionCheck =
        new TotalEventsProcessedCheck(
                spannerResourceManager,
                List.of(TABLE_NAME),
                gcsResourceManager,
                "output/dlq/severe/",
                3)
            .and(
                // Check that there is at least 1 error in DLQ
                DlqEventsCountCheck.builder(gcsResourceManager, "output/dlq/severe/")
                    .setMinEvents(1)
                    .build());
    assertTrue(conditionCheck.get());

    // Prepare for Live Retry
    String dlqGcsPrefix = bulkErrorFolderFullPath.replace("gs://" + artifactBucketName, "");
    SubscriptionName dlqSubscription =
        createPubsubResources(
            testName + "dlq", pubsubResourceManager, dlqGcsPrefix, gcsResourceManager);

    // Define Custom Transformation without Exception (Good)
    CustomTransformation customTransformationGood =
        CustomTransformation.builder(
                "customTransformation.jar", "com.custom.CustomTransformationAllTypes")
            .build();

    // launch forward migration template in retryDLQ mode
    retryLiveJobInfo =
        launchFwdDataflowJobInRetryDlqMode(
            spannerResourceManager,
            bulkErrorFolderFullPath,
            bulkErrorFolderFullPath + "/dlq",
            dlqSubscription,
            customTransformationGood);

    // Wait for Spanner to have all 3 rows
    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE_NAME)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(retryLiveJobInfo, Duration.ofMinutes(15)), conditionCheck);
    assertThatResult(result).meetsConditions();

    // Verify Data Content
    List<Map<String, Object>> expectedData = getExpectedData();
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("SELECT * FROM " + TABLE_NAME))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedData);
  }

  private void insertData() {
    // Row 1: Valid
    Map<String, Object> row1 = createRow(1, "valid1");
    // Row 2: Valid
    Map<String, Object> row2 = createRow(2, "valid2");
    // Row 3: Fail
    Map<String, Object> row3 = createRow(3, "fail_me");

    insertRow(row1);
    insertRow(row2);
    insertRow(row3);
  }

  private Map<String, Object> createRow(int id, String varcharVal) {
    Map<String, Object> row = new HashMap<>();
    row.put("id", id);
    row.put("varchar_col", varcharVal);
    row.put("tinyint_col", id);
    row.put("tinyint_unsigned_col", id);
    row.put("text_col", "text" + id);
    row.put("date_col", "2023-01-0" + id);
    row.put("smallint_col", 10 + id);
    row.put("smallint_unsigned_col", 10 + id);
    row.put("mediumint_col", 100 + id);
    row.put("mediumint_unsigned_col", 100 + id);
    row.put("bigint_col", 1000L + id);
    row.put("bigint_unsigned_col", 1000L + id);
    row.put("float_col", 1.5f + id);
    row.put("double_col", 2.5d + id);
    row.put("decimal_col", 10.5 + id);
    row.put("datetime_col", "2023-01-0" + id + " 12:00:00");
    row.put("time_col", "12:00:0" + id);
    row.put("year_col", 2023 + id);
    row.put("char_col", "c" + id);
    row.put("tinyblob_col", "blob" + id);
    row.put("tinytext_col", "tinytext" + id);
    row.put("blob_col", "blob" + id);
    row.put("mediumblob_col", "mediumblob" + id);
    row.put("mediumtext_col", "mediumtext" + id);
    row.put("test_json_col", "{\"k\":\"v" + id + "\"}");
    row.put("longblob_col", "longblob" + id);
    row.put("longtext_col", "longtext" + id);
    row.put("enum_col", "1");
    row.put("bool_col", true);
    // binary_col is padded, skipping for simplicity in insert but verified in expected if possible
    // row.put("binary_col", "bin" + id);
    row.put("varbinary_col", "varbin" + id);
    row.put("bit_col", id);
    row.put("bit8_col", id);
    row.put("bit1_col", 1);
    row.put("boolean_col", true);
    row.put("int_col", 1000 + id);
    row.put("integer_unsigned_col", 1000 + id);
    row.put("timestamp_col", "2023-01-0" + id + " 12:00:00");
    row.put("set_col", "v1");
    return row;
  }

  private void insertRow(Map<String, Object> row) {
    StringBuilder cols = new StringBuilder();
    StringBuilder vals = new StringBuilder();
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      if (cols.length() > 0) {
        cols.append(", ");
        vals.append(", ");
      }
      cols.append(entry.getKey());
      Object value = entry.getValue();
      if (value instanceof String) {
        vals.append("'").append(value).append("'");
      } else if (value instanceof Boolean) {
        vals.append((Boolean) value ? 1 : 0);
      } else {
        vals.append(value);
      }
    }
    String sql =
        String.format(
            "INSERT INTO %s (%s) VALUES (%s)", TABLE_NAME, cols.toString(), vals.toString());
    mySQLResourceManager.runSQLUpdate(sql);
  }

  private List<Map<String, Object>> getExpectedData() {
    List<Map<String, Object>> data = new ArrayList<>();
    data.add(createExpectedRow(1, "valid1"));
    data.add(createExpectedRow(2, "valid2"));
    data.add(createExpectedRow(3, "fail_me"));
    return data;
  }

  private Map<String, Object> createExpectedRow(int id, String varcharVal) {
    Map<String, Object> row = new HashMap<>();
    row.put("id", id);
    row.put("varchar_col", varcharVal);
    row.put("tinyint_col", id);
    row.put("tinyint_unsigned_col", id);
    row.put("text_col", "text" + id);
    row.put("date_col", "2023-01-0" + id);
    row.put("smallint_col", 10 + id);
    row.put("smallint_unsigned_col", 10 + id);
    row.put("mediumint_col", 100 + id);
    row.put("mediumint_unsigned_col", 100 + id);
    row.put("bigint_col", 1000L + id);
    row.put("bigint_unsigned_col", 1000L + id);
    row.put("float_col", (double) (1.5f + id));
    row.put("double_col", 2.5d + id);
    row.put("decimal_col", new java.math.BigDecimal(10.5 + id));
    row.put("datetime_col", "2023-01-0" + id + "T12:00:00Z");
    row.put("time_col", "12:00:0" + id);
    row.put("year_col", String.valueOf(2023 + id));
    row.put("char_col", "c" + id);

    String b64blob = java.util.Base64.getEncoder().encodeToString(("blob" + id).getBytes());
    row.put("tinyblob_col", b64blob);
    row.put("blob_col", b64blob);
    row.put(
        "mediumblob_col",
        java.util.Base64.getEncoder().encodeToString(("mediumblob" + id).getBytes()));

    row.put("tinytext_col", "tinytext" + id);
    row.put("mediumtext_col", "mediumtext" + id);
    row.put("test_json_col", "{\"k\":\"v" + id + "\"}");
    row.put(
        "longblob_col", java.util.Base64.getEncoder().encodeToString(("longblob" + id).getBytes()));
    row.put("longtext_col", "longtext" + id);
    row.put("enum_col", "1");
    row.put("bool_col", true);

    row.put(
        "varbinary_col", java.util.Base64.getEncoder().encodeToString(("varbin" + id).getBytes()));

    byte[] bitBytes = new byte[8];
    bitBytes[7] = (byte) id;
    row.put("bit_col", java.util.Base64.getEncoder().encodeToString(bitBytes));

    byte[] bit8Bytes = new byte[1];
    bit8Bytes[0] = (byte) id;
    row.put("bit8_col", java.util.Base64.getEncoder().encodeToString(bit8Bytes));

    row.put("bit1_col", true);
    row.put("boolean_col", true);
    row.put("int_col", 1000 + id);
    row.put("integer_unsigned_col", 1000 + id);
    row.put("timestamp_col", "2023-01-0" + id + "T12:00:00Z");
    row.put("set_col", "v1");

    return row;
  }

  private PipelineLauncher.LaunchInfo launchBulkDataflowJob(
      String jobName,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      CloudMySQLResourceManager mySQLResourceManager,
      CustomTransformation customTransformation)
      throws IOException {

    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
            .withTemplateName("Sourcedb_to_Spanner_Flex")
            .withTemplateModulePath("v2/sourcedb-to-spanner")
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("outputDirectory", getGcsPath("output", gcsResourceManager))
            .addParameter("sourceConfigURL", mySQLResourceManager.getUri())
            .addParameter("username", mySQLResourceManager.getUsername())
            .addParameter("password", mySQLResourceManager.getPassword())
            .addParameter("jdbcDriverClassName", "com.mysql.jdbc.Driver")
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("disable_runner_v2"));

    if (customTransformation != null) {
      flexTemplateBuilder.addParameter("transformationJarPath", customTransformation.jarPath());
      flexTemplateBuilder.addParameter("transformationClassName", customTransformation.classPath());
      if (customTransformation.customParameters() != null) {
        flexTemplateBuilder.addParameter(
            "transformationCustomParameters", customTransformation.customParameters());
      }
    }

    return flexTemplateBuilder.build().launchJob();
  }

  private PipelineLauncher.LaunchInfo launchFwdDataflowJobInRetryDlqMode(
      SpannerResourceManager spannerResourceManager,
      String bulkErrorFolderFullPath,
      String dlqGcsPath,
      SubscriptionName dlqSubscription,
      CustomTransformation customTransformation)
      throws IOException {

    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testName + "-retry")
            .withTemplateName("Cloud_Datastream_to_Spanner")
            .withTemplateModulePath("v2/datastream-to-spanner")
            .addParameter("inputFilePattern", bulkErrorFolderFullPath) // Not used in retryDLQ?
            // Actually, for retryDLQ, we might need to point to DLQ path or subscription.
            // The template param 'runMode'="retryDLQ" uses 'dlqGcsPubSubSubscription'.
            .addParameter(
                "streamName", "projects/testProject/locations/us-central1/streams/testStream")
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("deadLetterQueueDirectory", dlqGcsPath)
            .addParameter("dlqGcsPubSubSubscription", dlqSubscription.toString())
            .addParameter("datastreamSourceType", "mysql")
            .addParameter("inputFileFormat", "avro")
            .addParameter("runMode", "retryDLQ");

    if (customTransformation != null) {
      flexTemplateBuilder.addParameter("transformationJarPath", customTransformation.jarPath());
      flexTemplateBuilder.addParameter("transformationClassName", customTransformation.classPath());
      if (customTransformation.customParameters() != null) {
        flexTemplateBuilder.addParameter(
            "transformationCustomParameters", customTransformation.customParameters());
      }
    }

    return flexTemplateBuilder.build().launchJob();
  }

  public void createAndUploadJarToGcs(String gcsPathPrefix) throws IOException {
    String jarPath = "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
    gcsClient.uploadArtifact(gcsPathPrefix + "/customTransformation.jar", jarPath);
  }

  protected void loadSQLFileResource(
      org.apache.beam.it.jdbc.JDBCResourceManager jdbcResourceManager, String resourcePath)
      throws Exception {
    String sql =
        String.join(
            " ",
            com.google.common.io.Resources.readLines(
                com.google.common.io.Resources.getResource(resourcePath),
                java.nio.charset.StandardCharsets.UTF_8));
    loadSQLToJdbcResourceManager(jdbcResourceManager, sql);
  }

  protected void loadSQLToJdbcResourceManager(
      org.apache.beam.it.jdbc.JDBCResourceManager jdbcResourceManager, String sql)
      throws Exception {
    LOG.info("Loading sql to jdbc resource manager with uri: {}", jdbcResourceManager.getUri());
    try {
      java.sql.Connection connection =
          java.sql.DriverManager.getConnection(
              jdbcResourceManager.getUri(),
              jdbcResourceManager.getUsername(),
              jdbcResourceManager.getPassword());

      // Preprocess SQL to handle multi-line statements and newlines
      sql = sql.replaceAll("\r\n", " ").replaceAll("\n", " ");

      // Split into individual statements
      String[] statements = sql.split(";");

      // Execute each statement
      java.sql.Statement statement = connection.createStatement();
      for (String stmt : statements) {
        if (!stmt.trim().isEmpty()) {
          // Skip SELECT statements
          if (!stmt.trim().toUpperCase().startsWith("SELECT")) {
            LOG.info("Executing statement: {}", stmt);
            statement.executeUpdate(stmt);
          }
        }
      }
    } catch (Exception e) {
      LOG.info("failed to load SQL into database: {}", sql);
      throw new Exception("Failed to load SQL into database", e);
    }
    LOG.info("Successfully loaded sql to jdbc resource manager");
  }

  public CloudMySQLResourceManager setUpMySQLResourceManager() {
    return CloudMySQLResourceManager.builder(testName).build();
  }
}
