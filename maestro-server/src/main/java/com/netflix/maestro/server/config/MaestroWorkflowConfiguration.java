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
import com.netflix.maestro.engine.concurrency.TagPermitManager;
import com.netflix.maestro.engine.dao.MaestroStepBreakpointDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.dao.OutputDataDao;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.StepRuntimeCallbackDelayPolicy;
import com.netflix.maestro.engine.execution.StepRuntimeFixedCallbackDelayPolicy;
import com.netflix.maestro.engine.execution.StepRuntimeManager;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.StepSyncManager;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.SignalHandler;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.handlers.WorkflowInstanceActionHandler;
import com.netflix.maestro.engine.handlers.WorkflowRunner;
import com.netflix.maestro.engine.params.DefaultParamManager;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.steps.ForeachStepRuntime;
import com.netflix.maestro.engine.steps.NoOpStepRuntime;
import com.netflix.maestro.engine.steps.SleepStepRuntime;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.steps.SubworkflowStepRuntime;
import com.netflix.maestro.engine.tasks.MaestroEndTask;
import com.netflix.maestro.engine.tasks.MaestroGateTask;
import com.netflix.maestro.engine.tasks.MaestroStartTask;
import com.netflix.maestro.engine.tasks.MaestroTask;
import com.netflix.maestro.engine.tracing.MaestroTracingManager;
import com.netflix.maestro.engine.transformation.DagTranslator;
import com.netflix.maestro.engine.transformation.StepTranslator;
import com.netflix.maestro.engine.transformation.WorkflowTranslator;
import com.netflix.maestro.engine.utils.RollupAggregationHelper;
import com.netflix.maestro.engine.utils.WorkflowEnrichmentHelper;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.flow.runtime.ExecutionPreparer;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.server.properties.MaestroEngineProperties;
import com.netflix.maestro.server.properties.StepRuntimeProperties;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

/** Beans for maestro workflow classes. */
@SuppressWarnings("JavadocMethod")
@Configuration
@Slf4j
@EnableConfigurationProperties({StepRuntimeProperties.class})
public class MaestroWorkflowConfiguration {
  private static final String STEP_RUNTIME_QUALIFIER = "StepRuntimeMap";

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

  @Bean(name = STEP_RUNTIME_QUALIFIER)
  public EnumMap<StepType, StepRuntime> stepRuntimeMap() {
    LOG.info("Creating stepRuntimeMap within Spring boot...");
    return new EnumMap<>(StepType.class);
  }

  @Bean
  public NoOpStepRuntime noop(
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap) {
    LOG.info("Creating NoOp step within Spring boot...");
    NoOpStepRuntime step = new NoOpStepRuntime();
    stepRuntimeMap.put(StepType.NOOP, step);
    return step;
  }

  @Bean
  public SleepStepRuntime sleep(
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap) {
    LOG.info("Creating Sleep step within Spring boot...");
    SleepStepRuntime step = new SleepStepRuntime();
    stepRuntimeMap.put(StepType.SLEEP, step);
    return step;
  }

  @Bean
  public SubworkflowStepRuntime subworkflow(
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap,
      WorkflowActionHandler actionHandler,
      WorkflowInstanceActionHandler instanceActionHandler,
      InstanceStepConcurrencyHandler instanceStepConcurrencyHandler,
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao,
      StepRuntimeProperties stepRuntimeProperties) {
    LOG.info("Creating Subworkflow step within Spring boot...");
    Set<String> alwaysPassDownParamNames =
        Collections.unmodifiableSet(
            new HashSet<>(stepRuntimeProperties.getSubworkflow().getAlwaysPassDownParamNames()));
    SubworkflowStepRuntime step =
        new SubworkflowStepRuntime(
            actionHandler,
            instanceActionHandler,
            instanceStepConcurrencyHandler,
            instanceDao,
            stepInstanceDao,
            alwaysPassDownParamNames);
    stepRuntimeMap.put(StepType.SUBWORKFLOW, step);
    return step;
  }

  @Bean
  public ForeachStepRuntime foreach(
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap,
      WorkflowActionHandler actionHandler,
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao,
      MaestroStepInstanceActionDao actionDao,
      InstanceStepConcurrencyHandler instanceStepConcurrencyHandler,
      StepRuntimeProperties stepRuntimeProperties) {
    LOG.info(
        "Creating Foreach step with properties {} within Spring boot...",
        stepRuntimeProperties.getForeach());
    ForeachStepRuntime step =
        new ForeachStepRuntime(
            actionHandler,
            instanceDao,
            stepInstanceDao,
            actionDao,
            instanceStepConcurrencyHandler,
            stepRuntimeProperties.getForeach());
    stepRuntimeMap.put(StepType.FOREACH, step);
    return step;
  }

  @Bean
  public WorkflowRunner workflowRunner(
      FlowOperation flowOperation,
      WorkflowTranslator workflowTranslator,
      WorkflowHelper workflowHelper) {
    LOG.info("Creating Maestro WorkflowRunner within Spring boot...");
    return new WorkflowRunner(flowOperation, workflowTranslator, workflowHelper);
  }

  @Bean
  public ValidatorFactory validator() {
    LOG.info("Creating validator within Spring boot...");
    return Validation.byProvider(ApacheValidationProvider.class)
        .configure()
        .buildValidatorFactory();
  }

  @Bean
  public org.springframework.validation.Validator validatorFactory() {
    // Need this been for dependency injection to work in ConstraintValidator classes
    LOG.info("Creating validatorFactory within Spring boot...");
    return new LocalValidatorFactoryBean();
  }

  @Bean
  public WorkflowHelper getWorkflowHelper(
      ParamsManager paramsManager,
      ParamEvaluator paramEvaluator,
      DagTranslator dagTranslator,
      MaestroParamExtensionRepo paramExtensionRepo,
      MaestroJobEventPublisher maestroJobEventPublisher,
      MaestroEngineProperties properties) {
    LOG.info("Creating WorkflowHelper via spring boot...");
    return new WorkflowHelper(
        paramsManager,
        paramEvaluator,
        dagTranslator,
        paramExtensionRepo,
        maestroJobEventPublisher,
        properties.getMaxGroupNum());
  }

  @Bean
  public DryRunValidator getDryRunValidator(
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap,
      DefaultParamManager defaultParamManager,
      ParamsManager paramsManager,
      WorkflowHelper workflowHelper) {
    LOG.info("Creating DryRunValidator via spring boot...");
    return new DryRunValidator(stepRuntimeMap, defaultParamManager, paramsManager, workflowHelper);
  }

  @Bean
  public WorkflowEnrichmentHelper workflowEnrichmentHelper(
      ParamsManager paramsManager,
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap) {
    LOG.info("Creating WorkflowEnrichmentHelper within Spring boot...");
    return new WorkflowEnrichmentHelper(paramsManager, stepRuntimeMap);
  }

  @Bean
  public StepRuntimeCallbackDelayPolicy stepRuntimeCallbackPolicy(
      StepRuntimeProperties stepRuntimeProperties) {
    LOG.info("Creating StepRuntimeCallbackDelayPolicy policy within Spring boot...");
    return new StepRuntimeFixedCallbackDelayPolicy(stepRuntimeProperties.getCallbackDelayConfig());
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
      MaestroJobEventPublisher maestroJobEventPublisher,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      RollupAggregationHelper rollupAggregationHelper,
      MaestroMetrics metricRepo) {
    LOG.info("Creating Maestro endTask within Spring boot...");
    return new MaestroEndTask(
        instanceDao, maestroJobEventPublisher, objectMapper, rollupAggregationHelper, metricRepo);
  }

  @Bean
  public RollupAggregationHelper rollupAggregationHelper(MaestroStepInstanceDao stepInstanceDao) {
    LOG.info("Creating Maestro RollupAggregationHelper within Spring boot...");
    return new RollupAggregationHelper(stepInstanceDao);
  }

  @Bean
  @DependsOn({"sleep", "noop", "subworkflow", "foreach"})
  public StepRuntimeManager stepRuntimeManager(
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      ParamsManager paramsManager,
      MaestroMetrics metricRepo,
      MaestroTracingManager tracingManager) {
    LOG.info("Creating Maestro StepRuntimeManager within Spring boot...");
    return new StepRuntimeManager(
        stepRuntimeMap, objectMapper, paramsManager, metricRepo, tracingManager);
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
  public StepSyncManager stepSyncManager(
      MaestroStepInstanceDao instanceDao, MaestroJobEventPublisher publisher) {
    LOG.info("Creating Maestro StepSyncManager within Spring boot...");
    return new StepSyncManager(instanceDao, publisher);
  }

  @Bean
  public SignalHandler signalHandler() {
    LOG.info("Creating NoOp signalHandler within Spring boot...");
    return new SignalHandler() {
      @Override
      public boolean sendOutputSignals(
          WorkflowSummary workflowSummary, StepRuntimeSummary stepRuntimeSummary) {
        return true;
      }

      @Override
      public boolean signalsReady(
          WorkflowSummary workflowSummary, StepRuntimeSummary stepRuntimeSummary) {
        return true;
      }

      @Override
      public Map<String, List<Map<String, Parameter>>> getDependenciesParams(
          StepRuntimeSummary runtimeSummary) {
        return Collections.emptyMap();
      }
    };
  }

  @Bean
  public OutputDataManager outputParamsManager(OutputDataDao outputDataDao) {
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
}
