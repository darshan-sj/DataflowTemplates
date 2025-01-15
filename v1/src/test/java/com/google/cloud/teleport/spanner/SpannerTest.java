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
package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class SpannerTest {

  @Test
  public void testDevel() {
    String project = "span-cloud-migrations-staging";
    String instance = "teleport";
    String database = "test-db-new";
    // String host = "https://staging-wrenchworks.sandbox.googleapis.com";
    String host = "https://preprod-spanner.sandbox.googleapis.com";
    String query = "select 1";

    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder();
    optionsBuilder.setProjectId(project).setHost(host);

    Spanner spanner = optionsBuilder.build().getService();

    // try (ReadContext readContext =
    //     spanner.getDatabaseClient(DatabaseId.of(project, instance, database)).singleUse()) {
    //   ResultSet results = readContext.executeQuery(Statement.of(query));
    //
    //   ImmutableList.Builder<Struct> tableRecordsBuilder = ImmutableList.builder();
    //   while (results.next()) {
    //     tableRecordsBuilder.add(results.getCurrentRowAsStruct());
    //   }
    //   ImmutableList<Struct> tableRecords = tableRecordsBuilder.build();
    //   System.out.println("Successful");
    //
    // } catch (Exception e) {
    //   throw new SpannerResourceManagerException("Failed to read query " + query, e);
    // }

    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();

    try {
      databaseAdminClient
          .createDatabase(
              databaseAdminClient
                  .newDatabaseBuilder(DatabaseId.of(project, instance, database))
                  .setDialect(Dialect.GOOGLE_STANDARD_SQL)
                  .build(),
              ImmutableList.of())
          .get();
      System.out.println("Successful");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

    databaseAdminClient.dropDatabase(instance, database);
    System.out.println("Successfully dropped");
  }
}
