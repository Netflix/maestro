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
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.DatabaseSourceProvider;
import com.netflix.maestro.extensions.dao.MaestroForeachFlattenedDao;
import com.netflix.maestro.extensions.handlers.ForeachFlatteningHandler;
import com.netflix.maestro.extensions.listeners.SqsMaestroEventListener;
import com.netflix.maestro.extensions.metrics.SpectatorMaestroMetrics;
import com.netflix.maestro.extensions.processors.MaestroEventProcessor;
import com.netflix.maestro.extensions.processors.StepEventPreprocessor;
import com.netflix.maestro.extensions.properties.MaestroExtensionsProperties;
import com.netflix.maestro.extensions.provider.HttpMaestroDataProvider;
import com.netflix.maestro.extensions.provider.MaestroDataProvider;
import com.netflix.maestro.extensions.utils.ForeachFlatteningHelper;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.utils.JsonHelper;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import java.net.http.HttpClient;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Spring configuration for Maestro Extensions service beans. */
@Configuration
@EnableConfigurationProperties(MaestroExtensionsProperties.class)
@SuppressFBWarnings("EI_EXPOSE_REP2")
@Slf4j
public class MaestroExtensionsConfiguration {

  @Bean
  public ObjectMapper objectMapper() {
    LOG.info("Creating maestro objectMapper within Spring boot...");
    ObjectMapper mapper = JsonHelper.objectMapper();
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }

  /**
   * Creates a {@link DatabaseConfiguration} backed by Spring-bound properties. The YAML properties
   * under {@code extensions.database} are bound into a flat {@code Map<String, String>} and looked
   * up by converting the {@link DatabaseConfiguration} property keys (dot-separated) to kebab-case
   * keys. This follows the same pattern as {@code MaestroEngineProperties} in maestro-server.
   */
  @Bean
  @ConfigurationProperties(prefix = "extensions.database")
  public ExtensionsDatabaseProperties databaseConfiguration() {
    LOG.info("Creating DatabaseConfiguration within Spring boot...");
    return new ExtensionsDatabaseProperties();
  }

  @Bean(destroyMethod = "close")
  public DataSource dataSource(DatabaseConfiguration config) {
    LOG.info("Creating DataSource via DatabaseSourceProvider within Spring boot...");
    return new DatabaseSourceProvider(config).get();
  }

  @Bean
  public Registry registry() {
    return new DefaultRegistry();
  }

  @Bean
  public MaestroMetrics maestroMetrics(Registry registry) {
    LOG.info("Creating MaestroMetrics within Spring boot...");
    return new SpectatorMaestroMetrics(registry);
  }

  @Bean
  public HttpClient httpClient() {
    return HttpClient.newHttpClient();
  }

  @Bean
  public MaestroDataProvider maestroDataProvider(
      MaestroExtensionsProperties properties, ObjectMapper objectMapper, HttpClient httpClient) {
    LOG.info("Creating HttpMaestroDataProvider within Spring boot...");
    return new HttpMaestroDataProvider(properties.getMaestroBaseUrl(), objectMapper, httpClient);
  }

  @Bean
  public ForeachFlatteningHelper foreachFlatteningHelper() {
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
  public StepEventPreprocessor stepEventPreprocessor(
      MaestroDataProvider dataProvider, MaestroMetrics metrics) {
    LOG.info("Creating StepEventPreprocessor within Spring boot...");
    return new StepEventPreprocessor(dataProvider, metrics);
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
      MaestroEventProcessor processor, ObjectMapper objectMapper) {
    LOG.info("Creating SqsMaestroEventListener within Spring boot...");
    return new SqsMaestroEventListener(processor, objectMapper);
  }

  // ---------------------------------------------------------------------------
  // Inner class: DatabaseConfiguration impl
  // ---------------------------------------------------------------------------

  /**
   * Concrete {@link DatabaseConfiguration} implementation backed by a Spring-bound {@code
   * Map<String, String>}. Spring Boot binds YAML properties under {@code extensions.database} into
   * the {@code configs} map using relaxed binding (kebab-case keys). The {@link #getProperty}
   * method converts dot-separated keys from {@link DatabaseConfiguration} constants to kebab-case
   * for lookup.
   *
   * <p>This follows the same pattern as {@code MaestroEngineProperties} in maestro-server.
   */
  @AllArgsConstructor
  static class ExtensionsDatabaseProperties implements DatabaseConfiguration {
    private Map<String, String> configs;

    /** Default constructor for Spring Boot binding. */
    ExtensionsDatabaseProperties() {
      this.configs = new ConcurrentHashMap<>();
    }

    /** Setter used by Spring Boot's {@code @ConfigurationProperties} binding. */
    @SuppressWarnings("unused")
    public Map<String, String> getConfigs() {
      return configs;
    }

    /** Setter used by Spring Boot's {@code @ConfigurationProperties} binding. */
    @SuppressWarnings("unused")
    public void setConfigs(Map<String, String> configs) {
      this.configs = configs;
    }

    @Override
    public String getProperty(String name, String defaultValue) {
      String key = name.replace('.', '-').toLowerCase(Locale.US);
      return configs.getOrDefault(key, defaultValue);
    }
  }
}
