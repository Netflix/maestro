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
package com.netflix.maestro.extensions.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.extensions.dao.MaestroForeachFlattenedDao;
import com.netflix.maestro.extensions.handlers.ForeachFlatteningHandler;
import com.netflix.maestro.extensions.handlers.ForeachNotificationInterceptor;
import com.netflix.maestro.extensions.provider.DaoBackedMaestroDataProvider;
import com.netflix.maestro.extensions.provider.MaestroDataProvider;
import com.netflix.maestro.extensions.utils.ForeachFlatteningHelper;
import com.netflix.maestro.metrics.MaestroMetrics;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/** Spring configuration for foreach flattening beans. */
@Configuration
@Slf4j
public class ForeachFlattenConfiguration {

  @Bean
  public MaestroDataProvider maestroDataProvider(
      MaestroWorkflowDao workflowDao,
      MaestroWorkflowInstanceDao workflowInstanceDao,
      MaestroStepInstanceDao stepInstanceDao) {
    LOG.info("Creating MaestroDataProvider within Spring boot...");
    return new DaoBackedMaestroDataProvider(workflowDao, workflowInstanceDao, stepInstanceDao);
  }

  @Bean
  public ForeachFlatteningHelper foreachFlatteningHelper() {
    LOG.info("Creating ForeachFlatteningHelper within Spring boot...");
    return new ForeachFlatteningHelper();
  }

  @Bean
  public MaestroForeachFlattenedDao maestroForeachFlattenedDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    LOG.info("Creating MaestroForeachFlattenedDao within Spring boot...");
    return new MaestroForeachFlattenedDao(dataSource, objectMapper, config, metrics);
  }

  @Bean
  public ForeachFlatteningHandler foreachFlatteningHandler(
      MaestroDataProvider dataProvider,
      MaestroForeachFlattenedDao dao,
      ForeachFlatteningHelper helper) {
    LOG.info("Creating ForeachFlatteningHandler within Spring boot...");
    return new ForeachFlatteningHandler(dataProvider, dao, helper);
  }

  @Bean
  @Primary
  public MaestroNotificationPublisher foreachNotificationInterceptor(
      @Qualifier("notificationPublisher") MaestroNotificationPublisher delegate,
      MaestroDataProvider dataProvider,
      ForeachFlatteningHandler handler) {
    LOG.info("Creating ForeachNotificationInterceptor within Spring boot...");
    return new ForeachNotificationInterceptor(delegate, dataProvider, handler);
  }
}
