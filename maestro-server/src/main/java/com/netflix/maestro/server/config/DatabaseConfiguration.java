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
import com.netflix.maestro.database.DatabaseSourceProvider;
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
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.server.properties.MaestroEngineProperties;
import com.netflix.maestro.server.properties.MaestroProperties;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** beans for database related classes. */
@Slf4j
@Configuration
@EnableConfigurationProperties(MaestroEngineProperties.class)
public class DatabaseConfiguration {

  @Bean(name = "maestroDataSource")
  public DataSource maestroDataSource(MaestroEngineProperties props) {
    LOG.info("Creating maestroDataSource within Spring boot...");
    return new DatabaseSourceProvider(props).get();
  }

  // below are maestro flow DB Daos
  @Bean
  public MaestroFlowDao maestroFlowDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating IndexDAO within Spring boot...");
    return new MaestroFlowDao(crdbDataSource, objectMapper, props, metrics);
  }

  // below are maestro DB Daos
  @Bean
  public MaestroWorkflowDao maestroWorkflowDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroJobEventPublisher maestroJobEventPublisher,
      TriggerSubscriptionClient triggerSubscriptionClient,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroWorkflowDao within Spring boot...");
    return new MaestroWorkflowDao(
        maestroDataSource,
        objectMapper,
        props,
        maestroJobEventPublisher,
        triggerSubscriptionClient,
        metrics);
  }

  @Bean
  public MaestroWorkflowDeletionDao maestroWorkflowDeletionDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroWorkflowDeletionDao within Spring boot...");
    return new MaestroWorkflowDeletionDao(maestroDataSource, objectMapper, props, metrics);
  }

  @Bean
  public MaestroWorkflowInstanceDao maestroWorkflowInstanceDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroJobEventPublisher maestroJobEventPublisher,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroWorkflowInstanceDao within Spring boot...");
    return new MaestroWorkflowInstanceDao(
        maestroDataSource, objectMapper, props, maestroJobEventPublisher, metrics);
  }

  @Bean
  public MaestroRunStrategyDao maestroRunStrategyDao(
      DataSource crdbDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroJobEventPublisher maestroJobEventPublisher,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroRunStrategyDao within Spring boot...");
    return new MaestroRunStrategyDao(
        crdbDataSource, objectMapper, props, maestroJobEventPublisher, metrics);
  }

  @Bean
  public MaestroStepInstanceDao maestroStepInstanceDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroStepInstanceDAO within Spring boot...");
    return new MaestroStepInstanceDao(maestroDataSource, objectMapper, props, metrics);
  }

  @Bean
  public MaestroStepInstanceActionDao maestroInstanceActionDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroProperties maestroProperties,
      MaestroStepInstanceDao stepInstanceDao,
      MaestroJobEventPublisher maestroJobEventPublisher,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroInstanceActionDao within Spring boot...");
    return new MaestroStepInstanceActionDao(
        maestroDataSource,
        objectMapper,
        props,
        maestroProperties.getStepAction(),
        stepInstanceDao,
        maestroJobEventPublisher,
        metrics);
  }

  @Bean
  public OutputDataDao outputDataDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating outputDataDao within Spring boot...");
    return new OutputDataDao(maestroDataSource, objectMapper, props, metrics);
  }

  @Bean
  public MaestroStepBreakpointDao stepBreakpointDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroWorkflowDao workflowDao,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroStepBreakpointDao within Spring boot...");
    return new MaestroStepBreakpointDao(
        maestroDataSource, objectMapper, props, workflowDao, metrics);
  }
}
