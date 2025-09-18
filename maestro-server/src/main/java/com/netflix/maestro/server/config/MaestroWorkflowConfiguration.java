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

import brave.Span;
import brave.Tracer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.concurrency.MaestroTagPermitManager;
import com.netflix.maestro.engine.concurrency.TagPermitManager;
import com.netflix.maestro.engine.dao.MaestroOutputDataDao;
import com.netflix.maestro.engine.dao.MaestroStepBreakpointDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroTagPermitDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.StepRuntimeCallbackDelayPolicy;
import com.netflix.maestro.engine.execution.StepRuntimeManager;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.StepSyncManager;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.SignalHandler;
import com.netflix.maestro.engine.handlers.WorkflowRunner;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.tasks.MaestroEndTask;
import com.netflix.maestro.engine.tasks.MaestroGateTask;
import com.netflix.maestro.engine.tasks.MaestroStartTask;
import com.netflix.maestro.engine.tasks.MaestroTagPermitTask;
import com.netflix.maestro.engine.tasks.MaestroTask;
import com.netflix.maestro.engine.tracing.MaestroTracingManager;
import com.netflix.maestro.engine.transformation.DagTranslator;
import com.netflix.maestro.engine.transformation.StepTranslator;
import com.netflix.maestro.engine.transformation.WorkflowTranslator;
import com.netflix.maestro.engine.utils.RollupAggregationHelper;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.flow.runtime.ExecutionPreparer;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.server.properties.MaestroEngineProperties;
import com.netflix.maestro.server.properties.StepRuntimeProperties;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.handler.MaestroSignalHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

/** Beans for maestro workflow classes. */
@SuppressWarnings("JavadocMethod")
@Configuration
@Slf4j
public class MaestroWorkflowConfiguration {
  @Bean
  public WorkflowTranslator workflowTranslator(StepTranslator stepTranslator) {
    LOG.info("Creating Maestro workflowTranslator within Spring boot...");
    return new WorkflowTranslator(stepTranslator);
  }

  @Bean
  public DagTranslator dagTranslator(
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating Maestro dagTranslator within Spring boot...");
    return new DagTranslator(objectMapper);
  }

  @Bean
  public StepTranslator stepTranslator() {
    LOG.info("Creating Maestro stepTranslator within Spring boot...");
    return new StepTranslator();
  }

  @Bean
  public WorkflowRunner workflowRunner(
      MaestroFlowDao flowDao,
      FlowOperation flowOperation,
      WorkflowTranslator workflowTranslator,
      WorkflowHelper workflowHelper) {
    LOG.info("Creating Maestro WorkflowRunner within Spring boot...");
    return new WorkflowRunner(flowDao, flowOperation, workflowTranslator, workflowHelper);
  }

  @Bean
  public LocalValidatorFactoryBean localValidatorFactoryBean() {
    LOG.info("Creating localValidatorFactoryBean within Spring boot...");
    LocalValidatorFactoryBean bean = new LocalValidatorFactoryBean();
    bean.setProviderClass(ApacheValidationProvider.class);
    return bean;
  }

  @Bean
  public WorkflowHelper getWorkflowHelper(
      ParamsManager paramsManager,
      ParamEvaluator paramEvaluator,
      DagTranslator dagTranslator,
      MaestroParamExtensionRepo paramExtensionRepo,
      MaestroEngineProperties properties) {
    LOG.info("Creating WorkflowHelper via spring boot...");
    return new WorkflowHelper(
        paramsManager,
        paramEvaluator,
        dagTranslator,
        paramExtensionRepo,
        properties.getMaxGroupNum());
  }

  @Bean
  public MaestroTask maestroTask(
      StepRuntimeManager stepRuntimeManager,
      StepSyncManager stepSyncManager,
      ParamEvaluator paramEvaluator,
      SignalHandler signalClient,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      OutputDataManager outputDataManager,
      MaestroStepBreakpointDao stepBreakpointDao,
      MaestroStepInstanceActionDao actionDao,
      TagPermitManager tagPermitAcquirer,
      InstanceStepConcurrencyHandler instanceStepConcurrencyHandler,
      StepRuntimeCallbackDelayPolicy stepRuntimeCallbackDelayPolicy,
      MaestroMetrics metricRepo,
      MaestroTracingManager tracingManager,
      MaestroParamExtensionRepo extensionRepo) {
    LOG.info("Creating Maestro maestroTask within Spring boot...");
    return new MaestroTask(
        stepRuntimeManager,
        stepSyncManager,
        paramEvaluator,
        objectMapper,
        signalClient,
        outputDataManager,
        stepBreakpointDao,
        actionDao,
        tagPermitAcquirer,
        instanceStepConcurrencyHandler,
        stepRuntimeCallbackDelayPolicy,
        metricRepo,
        tracingManager,
        extensionRepo);
  }

  @Bean
  public MaestroStartTask maestroStartTask(
      MaestroWorkflowInstanceDao instanceDao,
      ExecutionPreparer executionPreparer,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating Maestro startTask within Spring boot...");
    return new MaestroStartTask(instanceDao, executionPreparer, objectMapper);
  }

  @Bean
  public MaestroGateTask maestroGateTask(
      MaestroStepInstanceDao stepInstanceDao,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating Maestro gateTask within Spring boot...");
    return new MaestroGateTask(stepInstanceDao, objectMapper);
  }

  @Bean
  public MaestroEndTask maestroEndTask(
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceActionDao actionDao,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      RollupAggregationHelper rollupAggregationHelper,
      MaestroMetrics metricRepo) {
    LOG.info("Creating Maestro endTask within Spring boot...");
    return new MaestroEndTask(
        instanceDao, actionDao, objectMapper, rollupAggregationHelper, metricRepo);
  }

  @Bean
  public MaestroTagPermitTask maestroTagPermitTask(
      MaestroTagPermitDao tagPermitDao,
      StepRuntimeProperties properties,
      MaestroQueueSystem queueSystem,
      MaestroMetrics metrics) {
    LOG.info("Creating Maestro TagPermitTask within Spring boot...");
    return new MaestroTagPermitTask(
        tagPermitDao, properties.getTagPermitTask(), queueSystem, metrics);
  }

  @Bean
  public RollupAggregationHelper rollupAggregationHelper(MaestroStepInstanceDao stepInstanceDao) {
    LOG.info("Creating Maestro RollupAggregationHelper within Spring boot...");
    return new RollupAggregationHelper(stepInstanceDao);
  }

  @Bean
  public MaestroTracingManager tracingManager(Tracer tracer) {
    LOG.info("Creating Maestro TracingManager within Spring boot...");
    return new MaestroTracingManager(tracer) {
      @Override
      public void tagInitSpan(
          Span initSpan, WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
        // noop
      }
    };
  }

  @Bean
  public StepSyncManager stepSyncManager(MaestroStepInstanceDao instanceDao) {
    LOG.info("Creating Maestro StepSyncManager within Spring boot...");
    return new StepSyncManager(instanceDao);
  }

  @Bean
  public SignalHandler signalHandler(MaestroSignalBrokerDao brokerDao) {
    LOG.info("Creating maestroSignalHandler within Spring boot...");
    return new MaestroSignalHandler(brokerDao);
  }

  @Bean
  public OutputDataManager outputParamsManager(MaestroOutputDataDao outputDataDao) {
    LOG.info("Creating OutputParamsManager within Spring boot...");
    return new OutputDataManager(outputDataDao);
  }

  @Bean
  @ConditionalOnProperty(
      value = "stepruntime.enable-tag-permit",
      havingValue = "false",
      matchIfMissing = true)
  public TagPermitManager noopTagPermitManager() {
    LOG.info("Creating maestro noop tagPermitManager within Spring boot...");
    return TagPermitManager.NOOP_TAG_PERMIT_MANAGER;
  }

  @Bean
  @ConditionalOnProperty(value = "stepruntime.enable-tag-permit", havingValue = "true")
  public TagPermitManager maestroTagPermitManager(
      MaestroTagPermitDao tagPermitDao, MaestroQueueSystem queueSystem) {
    LOG.info("Creating maestro TagPermitManager within Spring boot...");
    return new MaestroTagPermitManager(tagPermitDao, queueSystem);
  }
}
