/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.jdbc;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Custom class for the MySQL implementation of {@link AbstractJDBCResourceManager} abstract class.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is *
 * created when the container first spins up, if one is not given.
 *
 * <p>The class is thread-safe.
 *
 * <p>TODO - Remove module when https://github.com/apache/beam/pull/29732 is released.
 */
public class CustomMySQLResourceManager extends AbstractJDBCResourceManager<MySQLContainer<?>> {

  private static final String DEFAULT_MYSQL_CONTAINER_NAME = "mysql";

  // A list of available mySQL Docker image tags can be found at
  // https://hub.docker.com/_/mysql/tags?tab=tags
  private static final String DEFAULT_MYSQL_CONTAINER_TAG = "8.0.30";

  protected MySQLContainer container;

  private CustomMySQLResourceManager(Builder builder) {
    super(
        new MySQLContainer<>(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)).withExposedPorts(MySQLContainer.MYSQL_PORT),
        builder);
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  @Override
  protected int getJDBCPort() {
    return MySQLContainer.MYSQL_PORT;
  }

  @Override
  public String getJDBCPrefix() {
    return "mysql";
  }

  public int getPort() {
    return this.getPort(getJDBCPort());
  }

  public String getHost() {

  }

  /** Builder for {@link CustomMySQLResourceManager}. */
  public static final class Builder extends AbstractJDBCResourceManager.Builder<MySQLContainer<?>> {

    public Builder(String testId) {
      super(testId, DEFAULT_MYSQL_CONTAINER_NAME, DEFAULT_MYSQL_CONTAINER_TAG);
    }

    @Override
    public CustomMySQLResourceManager build() {
      return new CustomMySQLResourceManager(this);
    }
  }
}
