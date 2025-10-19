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
import com.netflix.maestro.engine.dao.MaestroJobTemplateDao;
import com.netflix.maestro.engine.dao.MaestroOutputDataDao;
import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroStepBreakpointDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroTagPermitDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDeletionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.dao.MaestroQueueDao;
import com.netflix.maestro.server.properties.MaestroEngineProperties;
import com.netflix.maestro.server.properties.MaestroProperties;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.dao.MaestroSignalInstanceDao;
import com.netflix.maestro.signal.dao.MaestroSignalParamDao;
import com.netflix.maestro.signal.dao.MaestroSignalTriggerDao;
import com.netflix.maestro.signal.producer.SignalQueueProducer;
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
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroFlowDao within Spring boot...");
    return new MaestroFlowDao(maestroDataSource, objectMapper, props, metrics);
  }

  // below are maestro queue DB Daos
  @Bean
  public MaestroQueueDao maestroQueueDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroQueueDao within Spring boot...");
    return new MaestroQueueDao(maestroDataSource, objectMapper, props, metrics);
  }

  // below are maestro DB Daos
  @Bean
  public MaestroWorkflowDao maestroWorkflowDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroQueueSystem queueSystem,
      TriggerSubscriptionClient triggerSubscriptionClient,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroWorkflowDao within Spring boot...");
    return new MaestroWorkflowDao(
        maestroDataSource, objectMapper, props, queueSystem, triggerSubscriptionClient, metrics);
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
      MaestroQueueSystem queueSystem,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroWorkflowInstanceDao within Spring boot...");
    return new MaestroWorkflowInstanceDao(
        maestroDataSource, objectMapper, props, queueSystem, metrics);
  }

  @Bean
  public MaestroRunStrategyDao maestroRunStrategyDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroQueueSystem queueSystem,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroRunStrategyDao within Spring boot...");
    return new MaestroRunStrategyDao(maestroDataSource, objectMapper, props, queueSystem, metrics);
  }

  @Bean
  public MaestroStepInstanceDao maestroStepInstanceDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroQueueSystem queueSystem,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroStepInstanceDAO within Spring boot...");
    return new MaestroStepInstanceDao(maestroDataSource, objectMapper, props, queueSystem, metrics);
  }

  @Bean
  public MaestroStepInstanceActionDao maestroInstanceActionDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroProperties maestroProperties,
      MaestroStepInstanceDao stepInstanceDao,
      MaestroQueueSystem queueSystem,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroInstanceActionDao within Spring boot...");
    return new MaestroStepInstanceActionDao(
        maestroDataSource,
        objectMapper,
        props,
        maestroProperties.getStepAction(),
        stepInstanceDao,
        queueSystem,
        metrics);
  }

  @Bean
  public MaestroOutputDataDao outputDataDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating outputDataDao within Spring boot...");
    return new MaestroOutputDataDao(maestroDataSource, objectMapper, props, metrics);
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

  @Bean
  public MaestroTagPermitDao tagPermitDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroTagPermitDao within Spring boot...");
    return new MaestroTagPermitDao(maestroDataSource, objectMapper, props, metrics);
  }

  @Bean
  public MaestroJobTemplateDao jobTemplateDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating MaestroJobTemplateDao within Spring boot...");
    return new MaestroJobTemplateDao(maestroDataSource, objectMapper, props, metrics);
  }

  // below are Maestro Signal Daos
  @Bean
  public MaestroSignalBrokerDao maestroSignalBrokerDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics,
      MaestroSignalInstanceDao instanceDao,
      MaestroSignalParamDao paramDao,
      MaestroSignalTriggerDao triggerDao,
      SignalQueueProducer queueProducer) {
    LOG.info("Creating maestroSignalBrokerDao within Spring boot...");
    return new MaestroSignalBrokerDao(
        maestroDataSource,
        objectMapper,
        props,
        metrics,
        instanceDao,
        paramDao,
        triggerDao,
        queueProducer);
  }

  @Bean
  public MaestroSignalInstanceDao maestroSignalInstanceDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroSignalInstanceDao within Spring boot...");
    return new MaestroSignalInstanceDao(maestroDataSource, objectMapper, props, metrics);
  }

  @Bean
  public MaestroSignalParamDao maestroSignalParamDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroSignalParamDao within Spring boot...");
    return new MaestroSignalParamDao(maestroDataSource, objectMapper, props, metrics);
  }

  @Bean
  public MaestroSignalTriggerDao maestroSignalTriggerDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroEngineProperties props,
      MaestroMetrics metrics) {
    LOG.info("Creating maestroSignalTriggerDao within Spring boot...");
    return new MaestroSignalTriggerDao(maestroDataSource, objectMapper, props, metrics);
  }
}
