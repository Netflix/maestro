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
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.eval.ExprEvaluator;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.handlers.StepInstanceActionHandler;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.handlers.WorkflowInstanceActionHandler;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.params.DefaultParamManager;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.server.properties.MaestroProperties;
import com.netflix.maestro.server.properties.StepRuntimeProperties;
import com.netflix.maestro.utils.JsonHelper;
import com.netflix.spectator.api.DefaultRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/** beans for maestro engine related classes. */
@Configuration
@Slf4j
@EnableCaching
@EnableConfigurationProperties({MaestroProperties.class, StepRuntimeProperties.class})
public class MaestroEngineConfiguration {
  private static final String OBJECT_MAPPER_WITH_YAML_QUALIFIER = "ObjectMapperWithYaml";

  @Primary
  @Bean(name = Constants.MAESTRO_QUALIFIER)
  public ObjectMapper objectMapper() {
    LOG.info("Creating maestro objectMapper within Spring boot...");
    return JsonHelper.objectMapper();
  }

  @Bean(name = OBJECT_MAPPER_WITH_YAML_QUALIFIER)
  public ObjectMapper objectMapperWithYaml() {
    LOG.info("Creating maestro YAML objectMapper within Spring boot...");
    return JsonHelper.objectMapperWithYaml();
  }

  @Bean
  public MaestroMetricRepo metricRepo() {
    LOG.info("Creating maestro metricRepo within Spring boot...");
    return new MaestroMetricRepo(new DefaultRegistry());
  }

  @Bean
  public WorkflowActionHandler workflowActionHandler(
      MaestroWorkflowDao workflowDao,
      MaestroWorkflowInstanceDao instanceDao,
      MaestroRunStrategyDao runStrategyDao,
      DryRunValidator dryRunValidator,
      WorkflowHelper workflowHelper) {
    LOG.info("Creating maestro workflowActionHandler within Spring boot...");
    return new WorkflowActionHandler(
        workflowDao, instanceDao, runStrategyDao, dryRunValidator, workflowHelper);
  }

  @Bean
  public WorkflowInstanceActionHandler workflowInstanceActionHandler(
      MaestroWorkflowDao workflowDao,
      MaestroWorkflowInstanceDao instanceDao,
      MaestroRunStrategyDao runStrategyDao,
      MaestroStepInstanceActionDao actionDao,
      WorkflowHelper workflowHelper) {
    LOG.info("Creating maestro workflowInstanceActionHandler within Spring boot...");
    return new WorkflowInstanceActionHandler(
        workflowDao, instanceDao, runStrategyDao, actionDao, workflowHelper);
  }

  @Bean
  public StepInstanceActionHandler stepInstanceActionHandler(
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceActionDao actionDao,
      WorkflowInstanceActionHandler actionHandler) {
    LOG.info("Creating maestro stepInstanceActionHandler within Spring boot...");
    return new StepInstanceActionHandler(instanceDao, actionDao, actionHandler);
  }

  @Bean
  @ConditionalOnProperty(
      value = "maestro.redis.enabled",
      havingValue = "false",
      matchIfMissing = true)
  public InstanceStepConcurrencyHandler noopInstanceStepConcurrencyHandler() {
    LOG.info("Creating maestro noop instanceStepConcurrencyHandler within Spring boot...");
    return InstanceStepConcurrencyHandler.NOOP_CONCURRENCY_HANDLER;
  }

  @Bean(initMethod = "init")
  public DefaultParamManager defaultParamManager(
      @Qualifier(OBJECT_MAPPER_WITH_YAML_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating DefaultParamManager within Spring boot...");
    return new DefaultParamManager(objectMapper);
  }

  @Bean
  public ParamsManager paramsManager(DefaultParamManager defaultParamManager) {
    LOG.info("Creating ParamsManager within Spring boot...");
    return new ParamsManager(defaultParamManager);
  }

  @Bean
  public MaestroParamExtensionRepo maestroParamExtensionRepo(
      MaestroStepInstanceDao stepInstanceDao,
      StepRuntimeProperties stepRuntimeProperties,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating Maestro MaestroParamExtensionRepo within Spring boot...");
    return new MaestroParamExtensionRepo(
        stepInstanceDao, stepRuntimeProperties.getEnv(), objectMapper);
  }

  @Bean(initMethod = "postConstruct", destroyMethod = "preDestroy")
  public ExprEvaluator exprEvaluator(
      MaestroProperties properties, MaestroParamExtensionRepo extensionRepo) {
    LOG.info("Creating maestro exprEvaluator within Spring boot...");
    return new ExprEvaluator(properties.getSel(), extensionRepo);
  }

  @Bean
  public ParamEvaluator paramEvaluatorHelper(
      ExprEvaluator exprEvaluator,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating maestro parameterHelper within Spring boot...");
    return new ParamEvaluator(exprEvaluator, objectMapper);
  }
}
