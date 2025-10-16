/*
 * Copyright 2025 Netflix, Inc.
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
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.StepRuntimeCallbackDelayPolicy;
import com.netflix.maestro.engine.execution.StepRuntimeFixedCallbackDelayPolicy;
import com.netflix.maestro.engine.execution.StepRuntimeManager;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.handlers.WorkflowInstanceActionHandler;
import com.netflix.maestro.engine.kubernetes.KubernetesCommandGenerator;
import com.netflix.maestro.engine.kubernetes.KubernetesRuntimeExecutor;
import com.netflix.maestro.engine.notebook.NotebookParamsBuilder;
import com.netflix.maestro.engine.notebook.PapermillEntrypointBuilder;
import com.netflix.maestro.engine.params.DefaultParamManager;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.stepruntime.KubernetesStepRuntime;
import com.netflix.maestro.engine.stepruntime.NotebookStepRuntime;
import com.netflix.maestro.engine.steps.ForeachStepRuntime;
import com.netflix.maestro.engine.steps.NoOpStepRuntime;
import com.netflix.maestro.engine.steps.SleepStepRuntime;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.steps.SubworkflowStepRuntime;
import com.netflix.maestro.engine.steps.WhileStepRuntime;
import com.netflix.maestro.engine.tracing.MaestroTracingManager;
import com.netflix.maestro.engine.utils.WorkflowEnrichmentHelper;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.server.properties.StepRuntimeProperties;
import com.netflix.maestro.server.runtime.Fabric8RuntimeExecutor;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

/** Beans for maestro workflow classes. */
@SuppressWarnings({"JavadocMethod", "PMD.LooseCoupling"})
@Configuration
@Slf4j
@EnableConfigurationProperties({StepRuntimeProperties.class})
public class MaestroStepRuntimeConfiguration {
  private static final String STEP_RUNTIME_QUALIFIER = "StepRuntimeMap";

  @Bean(name = STEP_RUNTIME_QUALIFIER)
  public EnumMap<StepType, StepRuntime> stepRuntimeMap() {
    LOG.info("Creating stepRuntimeMap within Spring boot...");
    return new EnumMap<>(StepType.class);
  }

  @Bean
  public KubernetesStepRuntime kubernetes(
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap,
      KubernetesRuntimeExecutor runtimeExecutor,
      KubernetesCommandGenerator commandGenerator,
      OutputDataManager outputDataManager,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroMetrics metrics) {
    LOG.info("Creating kubernetes step runtime within Spring boot...");
    KubernetesStepRuntime step =
        new KubernetesStepRuntime(
            runtimeExecutor, commandGenerator, outputDataManager, objectMapper, metrics);
    stepRuntimeMap.put(StepType.KUBERNETES, step);
    return step;
  }

  @Bean
  public KubernetesClient kubernetesClient() {
    LOG.info("Creating kubernetesClient within Spring boot...");
    return new KubernetesClientBuilder()
        .withConfig(new ConfigBuilder().withAutoConfigure().build())
        .build();
  }

  @Bean
  public KubernetesRuntimeExecutor kubernetesRuntimeExecutor(KubernetesClient kubernetesClient) {
    LOG.info("Creating kubernetesRuntimeExecutor within Spring boot...");
    return new Fabric8RuntimeExecutor(kubernetesClient);
  }

  @Bean
  public KubernetesCommandGenerator kubernetesCommandGenerator(
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating kubernetesCommandGenerator within Spring boot...");
    return new KubernetesCommandGenerator(objectMapper);
  }

  @Bean
  public NotebookStepRuntime notebook(
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap,
      KubernetesRuntimeExecutor runtimeExecutor,
      KubernetesCommandGenerator commandGenerator,
      OutputDataManager outputDataManager,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper,
      MaestroMetrics metrics,
      PapermillEntrypointBuilder entrypointBuilder) {
    LOG.info("Creating notebook step runtime within Spring boot...");
    NotebookStepRuntime step =
        new NotebookStepRuntime(
            runtimeExecutor,
            commandGenerator,
            outputDataManager,
            objectMapper,
            metrics,
            entrypointBuilder);
    stepRuntimeMap.put(StepType.NOTEBOOK, step);
    return step;
  }

  @Bean
  public PapermillEntrypointBuilder papermillEntrypointBuilder(
      NotebookParamsBuilder paramsBuilder) {
    LOG.info("Creating papermillEntrypointBuilder within Spring boot...");
    return new PapermillEntrypointBuilder(paramsBuilder);
  }

  @Bean
  public NotebookParamsBuilder notebookParamsBuilder(
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    LOG.info("Creating notebookParamsBuilder within Spring boot...");
    return new NotebookParamsBuilder(objectMapper);
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
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao,
      WorkflowInstanceActionHandler instanceActionHandler,
      MaestroQueueSystem queueSystem,
      InstanceStepConcurrencyHandler instanceStepConcurrencyHandler,
      StepRuntimeProperties stepRuntimeProperties) {
    LOG.info("Creating Subworkflow step within Spring boot...");
    Set<String> alwaysPassDownParamNames =
        Set.copyOf(stepRuntimeProperties.getSubworkflow().getAlwaysPassDownParamNames());
    SubworkflowStepRuntime step =
        new SubworkflowStepRuntime(
            actionHandler,
            instanceDao,
            stepInstanceDao,
            instanceActionHandler,
            queueSystem,
            instanceStepConcurrencyHandler,
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
      MaestroQueueSystem queueSystem,
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
            queueSystem,
            instanceStepConcurrencyHandler,
            stepRuntimeProperties.getForeach());
    stepRuntimeMap.put(StepType.FOREACH, step);
    return step;
  }

  @Bean
  public WhileStepRuntime whileLoop(
      @Qualifier(STEP_RUNTIME_QUALIFIER) Map<StepType, StepRuntime> stepRuntimeMap,
      WorkflowActionHandler actionHandler,
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao,
      MaestroQueueSystem queueSystem,
      InstanceStepConcurrencyHandler instanceStepConcurrencyHandler,
      ParamEvaluator paramEvaluator) {
    LOG.info("Creating While step within Spring boot...");
    WhileStepRuntime step =
        new WhileStepRuntime(
            actionHandler,
            instanceDao,
            stepInstanceDao,
            queueSystem,
            instanceStepConcurrencyHandler,
            paramEvaluator);
    stepRuntimeMap.put(StepType.WHILE, step);
    return step;
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
  @DependsOn({"sleep", "noop", "subworkflow", "foreach", "whileLoop", "kubernetes", "notebook"})
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
}
