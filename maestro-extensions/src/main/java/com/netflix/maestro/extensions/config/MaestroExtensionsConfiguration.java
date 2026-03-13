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
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.extensions.dao.MaestroForeachFlattenedDao;
import com.netflix.maestro.extensions.handlers.ForeachFlatteningHandler;
import com.netflix.maestro.extensions.listeners.SqsMaestroEventListener;
import com.netflix.maestro.extensions.processors.MaestroEventProcessor;
import com.netflix.maestro.extensions.processors.StepEventPreprocessor;
import com.netflix.maestro.extensions.properties.MaestroExtensionsProperties;
import com.netflix.maestro.extensions.provider.HttpMaestroClient;
import com.netflix.maestro.extensions.provider.MaestroClient;
import com.netflix.maestro.extensions.utils.ForeachFlatteningHelper;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import java.net.http.HttpClient;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for the Maestro Extensions module. Activates when {@code extensions.enabled}
 * is set to {@code true}. Injects shared beans (DataSource, ObjectMapper, MaestroMetrics,
 * DatabaseConfiguration) from the hosting application.
 */
@Configuration
@ComponentScan(basePackages = "com.netflix.maestro.extensions")
@EnableConfigurationProperties(MaestroExtensionsProperties.class)
@ConditionalOnProperty(value = "extensions.enabled", havingValue = "true")
@SuppressFBWarnings("EI_EXPOSE_REP2")
@Slf4j
public class MaestroExtensionsConfiguration {

  @Bean
  public MaestroClient maestroClient(
      MaestroExtensionsProperties properties,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      HttpClient httpClient) {
    LOG.info("Creating HttpMaestroClient within Spring boot...");
    return new HttpMaestroClient(properties.getMaestroBaseUrl(), objectMapper, httpClient);
  }

  @Bean
  public ForeachFlatteningHelper foreachFlatteningHelper() {
    return new ForeachFlatteningHelper();
  }

  @Bean
  public MaestroForeachFlattenedDao maestroForeachFlattenedDao(
      DataSource maestroDataSource,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    LOG.info("Creating MaestroForeachFlattenedDao within Spring boot...");
    return new MaestroForeachFlattenedDao(maestroDataSource, objectMapper, config, metrics);
  }

  @Bean
  public ForeachFlatteningHandler foreachFlatteningHandler(
      MaestroClient maestroClient, MaestroForeachFlattenedDao dao, ForeachFlatteningHelper helper) {
    LOG.info("Creating ForeachFlatteningHandler within Spring boot...");
    return new ForeachFlatteningHandler(maestroClient, dao, helper);
  }

  @Bean
  public StepEventPreprocessor stepEventPreprocessor(
      MaestroClient maestroClient, MaestroMetrics metrics) {
    LOG.info("Creating StepEventPreprocessor within Spring boot...");
    return new StepEventPreprocessor(maestroClient, metrics);
  }

  @Bean
  public MaestroEventProcessor maestroEventProcessor(
      StepEventPreprocessor preprocessor,
      ForeachFlatteningHandler handler,
      MaestroMetrics metrics) {
    LOG.info("Creating MaestroEventProcessor within Spring boot...");
    return new MaestroEventProcessor(preprocessor, handler, metrics);
  }

  @Bean
  public SqsMaestroEventListener sqsMaestroEventListener(
      MaestroEventProcessor processor,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating SqsMaestroEventListener within Spring boot...");
    return new SqsMaestroEventListener(processor, objectMapper);
  }
}
