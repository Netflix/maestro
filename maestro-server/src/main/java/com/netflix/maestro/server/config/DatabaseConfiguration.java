/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.cockroachdb.CockroachDBDataSourceProvider;
import com.netflix.conductor.cockroachdb.dao.CockroachDBEventHandlerDAO;
import com.netflix.conductor.cockroachdb.dao.CockroachDBIndexDAO;
import com.netflix.conductor.cockroachdb.dao.CockroachDBMetadataDAO;
import com.netflix.conductor.cockroachdb.dao.CockroachDBPollDataDAO;
import com.netflix.conductor.cockroachdb.dao.CockroachDBRateLimitingDAO;
import com.netflix.conductor.cockroachdb.dao.MaestroCockroachDBExecutionDao;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import com.netflix.maestro.engine.compression.GZIPCompressor;
import com.netflix.maestro.engine.compression.StringCodec;
import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroStepBreakpointDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDeletionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.dao.OutputDataDao;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.server.properties.ConductorProperties;
import java.util.Collections;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** beans for database related classes. */
@Slf4j
@Configuration
@EnableConfigurationProperties(ConductorProperties.class)
public class DatabaseConfiguration {

  @Bean(name = "crdbDataSource")
  public DataSource crdbDataSource(ConductorProperties props) {
    LOG.info("Creating crdbDataSource within Spring boot...");
    return new CockroachDBDataSourceProvider(props).get();
  }

  // below are conductor DB DAOs
  @Bean
  public MetadataDAO metadataDAO(
      DataSource crdbDataSource,
      @Qualifier(ConductorConfiguration.CONDUCTOR_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props) {
    LOG.info("Creating metadataDAO within Spring boot...");
    return new CockroachDBMetadataDAO(crdbDataSource, objectMapper, props);
  }

  @Bean
  public EventHandlerDAO eventHandlerDAO(
      DataSource crdbDataSource,
      @Qualifier(ConductorConfiguration.CONDUCTOR_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props) {
    LOG.info("Creating eventHandlerDAO within Spring boot...");
    return new CockroachDBEventHandlerDAO(crdbDataSource, objectMapper, props);
  }

  @Bean
  public ExecutionDAO executionDAO(
      DataSource crdbDataSource,
      IndexDAO indexDAO,
      @Qualifier(ConductorConfiguration.CONDUCTOR_QUALIFIER) ObjectMapper objectMapper,
      StringCodec stringCodec,
      ConductorProperties props) {
    LOG.info("Creating executionDAO within Spring boot...");
    return new MaestroCockroachDBExecutionDao(
        crdbDataSource, objectMapper, stringCodec, indexDAO, props);
  }

  @Bean
  public StringCodec stringCodec() {
    return new StringCodec(Collections.singletonList(new GZIPCompressor()));
  }

  @Bean
  public RateLimitingDAO rateLimitingDAO(
      DataSource crdbDataSource,
      @Qualifier(ConductorConfiguration.CONDUCTOR_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props) {
    LOG.info("Creating rateLimitingDao within Spring boot...");
    return new CockroachDBRateLimitingDAO(crdbDataSource, objectMapper, props);
  }

  @Bean
  public PollDataDAO pollDataDAO(
      DataSource crdbDataSource,
      @Qualifier(ConductorConfiguration.CONDUCTOR_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props) {
    LOG.info("Creating pollDataDAO within Spring boot...");
    return new CockroachDBPollDataDAO(crdbDataSource, objectMapper, props);
  }

  @Bean
  public IndexDAO indexDAO(
      DataSource crdbDataSource,
      @Qualifier(ConductorConfiguration.CONDUCTOR_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props) {
    LOG.info("Creating IndexDAO within Spring boot...");
    return new CockroachDBIndexDAO(crdbDataSource, objectMapper, props);
  }

  // below are maestro DB Daos
  @Bean
  public MaestroWorkflowDao maestroWorkflowDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props,
      MaestroJobEventPublisher maestroJobEventPublisher,
      TriggerSubscriptionClient triggerSubscriptionClient) {
    LOG.info("Creating maestroWorkflowDao within Spring boot...");
    return new MaestroWorkflowDao(
        crdbDataSource, objectMapper, props, maestroJobEventPublisher, triggerSubscriptionClient);
  }

  @Bean
  public MaestroWorkflowDeletionDao maestroWorkflowDeletionDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props) {
    LOG.info("Creating maestroWorkflowDeletionDao within Spring boot...");
    return new MaestroWorkflowDeletionDao(crdbDataSource, objectMapper, props);
  }

  @Bean
  public MaestroWorkflowInstanceDao maestroWorkflowInstanceDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props,
      MaestroJobEventPublisher maestroJobEventPublisher) {
    LOG.info("Creating maestroWorkflowInstanceDao within Spring boot...");
    return new MaestroWorkflowInstanceDao(
        crdbDataSource, objectMapper, props, maestroJobEventPublisher);
  }

  @Bean
  public MaestroRunStrategyDao maestroRunStrategyDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props,
      MaestroJobEventPublisher maestroJobEventPublisher,
      MaestroMetrics metricRepo) {
    LOG.info("Creating maestroRunStrategyDao within Spring boot...");
    return new MaestroRunStrategyDao(
        crdbDataSource, objectMapper, props, maestroJobEventPublisher, metricRepo);
  }

  @Bean
  public MaestroStepInstanceDao maestroStepInstanceDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props) {
    LOG.info("Creating maestroStepInstanceDAO within Spring boot...");
    return new MaestroStepInstanceDao(crdbDataSource, objectMapper, props);
  }

  @Bean
  public MaestroStepInstanceActionDao maestroInstanceActionDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props,
      MaestroStepInstanceDao stepInstanceDao,
      MaestroJobEventPublisher maestroJobEventPublisher) {
    LOG.info("Creating maestroInstanceActionDao within Spring boot...");
    return new MaestroStepInstanceActionDao(
        crdbDataSource, objectMapper, props, stepInstanceDao, maestroJobEventPublisher);
  }

  @Bean
  public OutputDataDao outputDataDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props) {
    LOG.info("Creating outputDataDao within Spring boot...");
    return new OutputDataDao(crdbDataSource, objectMapper, props);
  }

  @Bean
  public MaestroStepBreakpointDao stepBreakpointDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      ConductorProperties props,
      MaestroWorkflowDao workflowDao) {
    LOG.info("Creating maestroStepBreakpointDao within Spring boot...");
    return new MaestroStepBreakpointDao(crdbDataSource, objectMapper, props, workflowDao);
  }
}
