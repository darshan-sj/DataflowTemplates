/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraAnnotations;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;

public interface ISchemaMapper extends Serializable {
  /** Retrieves the Spanner dialect. */
  Dialect getDialect();

  /**
   * Return the source tables configured for migration.
   *
   * @param namespace is currently not operational.
   * @return
   */
  List<String> getSourceTablesToMigrate(String namespace);

  /**
   * Retrieves the corresponding Source table name given a spanner table name.
   *
   * @param namespace is currently not operational.
   */
  String getSourceTableName(String namespace, String spTable) throws NoSuchElementException;

  /**
   * Retrieves the corresponding Spanner table name given a source table name.
   *
   * @param namespace is currently not operational.
   */
  String getSpannerTableName(String namespace, String srcTable) throws NoSuchElementException;

  /**
   * Retrieves the corresponding Spanner column name given a source table and source column.
   *
   * @param namespace is currently not operational.
   */
  String getSpannerColumnName(String namespace, String srcTable, String srcColumn)
      throws NoSuchElementException;

  /**
   * Retrieves the corresponding source column name given a Spanner table and Spanner column.
   *
   * @param namespace is currently not operational.
   */
  String getSourceColumnName(String namespace, String spannerTable, String spannerColumn)
      throws NoSuchElementException;

  /**
   * Retrieves the Spanner column data type given a spanner table and spanner column.
   *
   * @param namespace is currently not operational.
   */
  Type getSpannerColumnType(String namespace, String spannerTable, String spannerColumn)
      throws NoSuchElementException;

  /**
   * Retrieves the Spanner column's Cassandra annotation given a spanner table and spanner column.
   *
   * @param namespace is currently not operational.
   */
  CassandraAnnotations getSpannerColumnCassandraAnnotations(
      String namespace, String spannerTable, String spannerColumn) throws NoSuchElementException;

  /**
   * Retrieves a list of all column names within a Spanner table.
   *
   * @param namespace is currently not operational.
   */
  List<String> getSpannerColumns(String namespace, String spannerTable)
      throws NoSuchElementException;

  /**
   * Retrieves the name of the shard id column for a Spanner table.
   *
   * @param namespace is currently not operational.
   */
  String getShardIdColumnName(String namespace, String spannerTableName);

  /**
   * Retrieves the name of the synthetic primary key column for a Spanner table.
   *
   * @param namespace is currently not operational.
   */
  String getSyntheticPrimaryKeyColName(String namespace, String spannerTableName);

  /**
   * Returns true if a corresponding source column exists for the provided Spanner column.
   *
   * @param namespace is currently not operational.
   */
  boolean colExistsAtSource(String namespace, String spannerTable, String spannerColumn);
}
