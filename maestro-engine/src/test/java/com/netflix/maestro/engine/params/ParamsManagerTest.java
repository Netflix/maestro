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
package com.netflix.maestro.engine.params;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.eval.InstanceWrapper;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.initiator.SubworkflowInitiator;
import com.netflix.maestro.models.initiator.TimeInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.LongParamDefinition;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.models.signal.SignalDependenciesDefinition;
import com.netflix.maestro.models.signal.SignalOutputsDefinition;
import com.netflix.maestro.utils.JsonHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ParamsManagerTest extends MaestroEngineBaseTest {
  private @Mock DefaultParamManager defaultParamManager;
  private ParamsManager paramsManager;
  private WorkflowSummary workflowSummary;
  private ObjectMapper yamlMapper;
  private Workflow workflow;
  private DefaultParamManager defaultsManager;
  private StepRuntimeSummary runtimeSummary;
  private Step step;
  private StepRuntime stepRuntime;
  private WorkflowInstance workflowInstance;

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  @Before
  public void setUp() throws IOException {
    yamlMapper = JsonHelper.objectMapperWithYaml();
    defaultParamManager = Mockito.mock(DefaultParamManager.class);
    paramsManager = new ParamsManager(defaultParamManager);
    defaultsManager = new DefaultParamManager(yamlMapper);
    defaultsManager.init();
    workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId("abc");
    workflowSummary.setWorkflowInstanceId(123);
    runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId("stepid")
            .stepInstanceUuid("123")
            .stepAttemptId(1)
            .build();
    stepRuntime = Mockito.mock(StepRuntime.class);
    step = new TypedStep();
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-notebook.json", WorkflowDefinition.class);
    workflow = definition.getWorkflow();
    RunProperties runProperties = new RunProperties();
    runProperties.setOwner(User.create("demo"));
    workflowInstance = new WorkflowInstance();
    workflowInstance.setWorkflowInstanceId(123);
    workflowInstance.setRuntimeWorkflow(definition.getWorkflow());
    workflowInstance.setRunProperties(runProperties);
  }

  @Test
  public void testGetStepDependencyParams() throws Exception {
    SignalDependenciesDefinition signalDependencies =
        loadObject(
            "fixtures/parameters/signal-dependency-params.json",
            SignalDependenciesDefinition.class);
    List<MapParameter> signalParams =
        ParamsManager.getSignalDependenciesParameters(signalDependencies);
    Assert.assertEquals(1, signalParams.size());
    Assert.assertEquals("signal_a", signalParams.getFirst().getValue().get("name").getValue());
    Assert.assertEquals("bar", signalParams.getFirst().getValue().get("foo").getValue());

    // same def can be loaded for outputs, the operator fields will be ignored.
    SignalOutputsDefinition signalOutputs =
        loadObject("fixtures/parameters/signal-output-params.json", SignalOutputsDefinition.class);
    signalParams = ParamsManager.getSignalOutputsParameters(signalOutputs);
    Assert.assertEquals(1, signalParams.size());
    Assert.assertEquals("signal_a", signalParams.getFirst().getValue().get("name").getValue());
    Assert.assertEquals("bar", signalParams.getFirst().getValue().get("foo").getValue());

    // empty check
    Assert.assertTrue(ParamsManager.getSignalOutputsParameters(null).isEmpty());
    Assert.assertTrue(
        ParamsManager.getSignalOutputsParameters(new SignalOutputsDefinition(null)).isEmpty());
  }

  @Test
  public void testStepParamSanity() {
    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);
    Assert.assertTrue(stepParams.isEmpty());
    when(defaultParamManager.getDefaultStepParams())
        .thenReturn(
            Collections.singletonMap(
                "workflow_id",
                ParamDefinition.buildParamDefinition("workflow_id", "test-workflow")));
    stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);
    Assert.assertFalse(stepParams.isEmpty());
  }

  @Test
  public void testMergeStepRunParamFromWorkflowSummary() {
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        singletonMap(
            "stepid", singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));
    workflowSummary.setStepRunParams(stepRunParams);
    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);
    Assert.assertFalse(stepParams.isEmpty());
    Assert.assertEquals("d1", stepParams.get("p1").asStringParam().getValue());
  }

  @Test
  public void testMergeRunParamFromWorkflowSummaryWithUpstreamInitiator() {
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        singletonMap(
            "stepid", singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));
    ParamSource[] expectedSources =
        new ParamSource[] {
          ParamSource.FOREACH, ParamSource.WHILE, ParamSource.SUBWORKFLOW, ParamSource.TEMPLATE
        };
    Initiator.Type[] initiators =
        new Initiator.Type[] {
          Initiator.Type.FOREACH,
          Initiator.Type.WHILE,
          Initiator.Type.SUBWORKFLOW,
          Initiator.Type.TEMPLATE
        };
    for (int i = 0; i < initiators.length; i++) {
      UpstreamInitiator upstreamInitiator = UpstreamInitiator.withType(initiators[i]);
      workflowSummary.setInitiator(upstreamInitiator);
      workflowSummary.setStepRunParams(stepRunParams);
      Map<String, Parameter> stepParams =
          paramsManager.generateMergedStepParams(
              workflowSummary, step, stepRuntime, runtimeSummary);
      Assert.assertFalse(stepParams.isEmpty());
      Assert.assertEquals("d1", stepParams.get("p1").asStringParam().getValue());
      Assert.assertEquals(expectedSources[i], stepParams.get("p1").getSource());
    }
  }

  @Test
  public void testMergeRunParamFromWorkflowSummaryWithStartRestart() {
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        singletonMap(
            "stepid", singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));
    ManualInitiator manualInitiator = new ManualInitiator();
    workflowSummary.setInitiator(manualInitiator);
    workflowSummary.setStepRunParams(stepRunParams);

    workflowSummary.setRunPolicy(RunPolicy.START_FRESH_NEW_RUN);
    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);
    Assert.assertFalse(stepParams.isEmpty());
    Assert.assertEquals("d1", stepParams.get("p1").asStringParam().getValue());
    Assert.assertEquals(ParamSource.LAUNCH, stepParams.get("p1").getSource());

    // restart should propagate to source
    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);
    Assert.assertEquals(ParamSource.RESTART, stepParams.get("p1").getSource());
  }

  @Test
  public void testStepParamFromStepSummary() {
    Map<String, ParamDefinition> stepRunParams =
        singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1"));
    Map<String, Map<String, ParamDefinition>> allStepRunParams =
        singletonMap(
            "stepid", singletonMap("p2", ParamDefinition.buildParamDefinition("p2", "d1")));

    workflowSummary.setStepRunParams(allStepRunParams);
    runtimeSummary.setStepRunParams(stepRunParams);
    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);
    Assert.assertFalse(stepParams.isEmpty());

    // should only get step summary if both in workflow and step
    Assert.assertEquals("d1", stepParams.get("p1").asStringParam().getValue());
    Assert.assertNull(stepParams.get("p2"));
  }

  @Test
  public void testRestartWithSystemInjectedConstantStepParam() {
    ParamDefinition param =
        StringParamDefinition.builder()
            .name("AUTHORIZED_MANAGERS")
            .value("test-auth-manager")
            .addMetaField(Constants.METADATA_SOURCE_KEY, ParamSource.SYSTEM_INJECTED.name())
            .mode(ParamMode.CONSTANT)
            .build();
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        singletonMap(
            "stepid",
            twoItemMap(
                "p1", ParamDefinition.buildParamDefinition("p1", "d1"), param.getName(), param));

    workflowSummary.setStepRunParams(stepRunParams);
    runtimeSummary.setRestartConfig(RestartConfig.builder().build());
    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);
    Assert.assertFalse(stepParams.isEmpty());
    Assert.assertEquals("d1", stepParams.get("p1").asStringParam().getValue());
    Assert.assertEquals(
        "test-auth-manager", stepParams.get("AUTHORIZED_MANAGERS").asStringParam().getValue());
    Assert.assertEquals(ParamMode.CONSTANT, stepParams.get("AUTHORIZED_MANAGERS").getMode());
    Assert.assertEquals(ParamSource.RESTART, stepParams.get("AUTHORIZED_MANAGERS").getSource());
  }

  @Test
  public void testInjectedJobTemplateParams() {
    when(stepRuntime.injectRuntimeParams(any(), any()))
        .thenReturn(singletonMap("p2", ParamDefinition.buildParamDefinition("p2", "d2")));
    when(defaultParamManager.getDefaultParamsForType(any()))
        .thenReturn(
            Optional.of(singletonMap("p3", ParamDefinition.buildParamDefinition("p3", "d3"))));

    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);

    Assert.assertTrue(stepParams.containsKey("p2"));
    Assert.assertTrue(stepParams.containsKey("p3"));
    Assert.assertEquals("d2", stepParams.get("p2").asStringParam().getValue());
    Assert.assertEquals("d3", stepParams.get("p3").asStringParam().getValue());
    Assert.assertEquals(ParamSource.TEMPLATE_SCHEMA, stepParams.get("p2").getSource());
    Assert.assertEquals(ParamSource.SYSTEM_DEFAULT, stepParams.get("p3").getSource());
  }

  @Test
  public void testInjectedJobTemplateParamsOverride() {
    when(stepRuntime.injectRuntimeParams(any(), any()))
        .thenReturn(singletonMap("p3", ParamDefinition.buildParamDefinition("p3", "d2")));
    when(defaultParamManager.getDefaultParamsForType(any()))
        .thenReturn(
            Optional.of(singletonMap("p3", ParamDefinition.buildParamDefinition("p3", "d3"))));

    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);

    Assert.assertTrue(stepParams.containsKey("p3"));
    Assert.assertEquals("d2", stepParams.get("p3").asStringParam().getValue());
    Assert.assertEquals(ParamSource.TEMPLATE_SCHEMA, stepParams.get("p3").getSource());
  }

  @Test
  public void testInjectedJobTemplateParamsOverridden() {
    when(stepRuntime.injectRuntimeParams(any(), any()))
        .thenReturn(singletonMap("p3", ParamDefinition.buildParamDefinition("p3", "d2")));
    ((TypedStep) step)
        .setParams(singletonMap("p3", ParamDefinition.buildParamDefinition("p3", "d1")));

    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);

    Assert.assertTrue(stepParams.containsKey("p3"));
    Assert.assertEquals("d1", stepParams.get("p3").asStringParam().getValue());
    Assert.assertEquals(ParamSource.DEFINITION, stepParams.get("p3").getSource());
  }

  @Test
  public void testWorkflowParamSanity() {
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .runParams(params)
            .build();
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);
    Assert.assertFalse(workflowParams.isEmpty());
  }

  @Test
  public void testWorkflowParamRunParamsUpstreamInitiator() {
    Map<String, ParamDefinition> runParams =
        singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1"));

    ParamSource[] expectedSources =
        new ParamSource[] {
          ParamSource.FOREACH, ParamSource.WHILE, ParamSource.SUBWORKFLOW, ParamSource.TEMPLATE
        };
    Initiator.Type[] initiators =
        new Initiator.Type[] {
          Initiator.Type.FOREACH,
          Initiator.Type.WHILE,
          Initiator.Type.SUBWORKFLOW,
          Initiator.Type.TEMPLATE
        };
    for (int i = 0; i < initiators.length; i++) {
      RunRequest request =
          RunRequest.builder()
              .initiator(UpstreamInitiator.withType(initiators[i]))
              .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
              .runParams(runParams)
              .build();
      Map<String, Parameter> workflowParams =
          paramsManager.generateMergedWorkflowParams(workflowInstance, request);
      Assert.assertFalse(workflowParams.isEmpty());
      Assert.assertEquals("d1", workflowParams.get("p1").asStringParam().getValue());
      Assert.assertEquals(expectedSources[i], workflowParams.get("p1").getSource());
    }
  }

  @Test
  public void testWorkflowParamRunParamsUpstreamMergeInitiator() {
    Map<String, ParamDefinition> defaultParams =
        singletonMap(
            "TARGET_RUN_DATE",
            ParamDefinition.buildParamDefinition("TARGET_RUN_DATE", 1000).toBuilder()
                .mode(ParamMode.MUTABLE_ON_START)
                .build());
    Map<String, ParamDefinition> runParams =
        singletonMap(
            "TARGET_RUN_DATE",
            ParamDefinition.buildParamDefinition("TARGET_RUN_DATE", 1001).toBuilder()
                .mode(ParamMode.MUTABLE)
                .meta(singletonMap(Constants.METADATA_SOURCE_KEY, "DEFINITION"))
                .build());
    when(defaultParamManager.getDefaultWorkflowParams()).thenReturn(defaultParams);

    ParamSource[] expectedSources =
        new ParamSource[] {
          ParamSource.FOREACH, ParamSource.WHILE, ParamSource.SUBWORKFLOW, ParamSource.TEMPLATE
        };
    Initiator.Type[] initiators =
        new Initiator.Type[] {
          Initiator.Type.FOREACH,
          Initiator.Type.WHILE,
          Initiator.Type.SUBWORKFLOW,
          Initiator.Type.TEMPLATE
        };
    for (int i = 0; i < initiators.length; i++) {
      RunRequest request =
          RunRequest.builder()
              .initiator(UpstreamInitiator.withType(initiators[i]))
              .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
              .runParams(runParams)
              .build();
      Map<String, Parameter> workflowParams =
          paramsManager.generateMergedWorkflowParams(workflowInstance, request);
      Assert.assertFalse(workflowParams.isEmpty());
      Assert.assertEquals(
          1001L, (long) workflowParams.get("TARGET_RUN_DATE").asLongParam().getValue());
      Assert.assertEquals(expectedSources[i], workflowParams.get("TARGET_RUN_DATE").getSource());
    }
  }

  /**
   * This is a test that should fail because even upstream merge cannot override params with modes
   * that are excluded from merging context. For example, [MUTABLE_ON_START] param cannot be
   * modified when upstream restart happens. However, currently this is allowed due to unclear of
   * the param source definition. Leave the tests as they are and will update once the param source
   * issue is fixed.
   */
  @Test
  public void testWorkflowParamRunParamsUpstreamInitiatorRestartMerge() {
    Map<String, ParamDefinition> restartParams =
        singletonMap(
            "TARGET_RUN_DATE",
            ParamDefinition.buildParamDefinition("TARGET_RUN_DATE", 1001).toBuilder()
                .mode(ParamMode.MUTABLE)
                .meta(singletonMap(Constants.METADATA_SOURCE_KEY, "RESTART"))
                .build());
    Map<String, Parameter> instanceParams = new LinkedHashMap<>();
    instanceParams.put("RUN_TS", buildParam("RUN_TS", 123L));
    instanceParams.put(
        "TARGET_RUN_DATE",
        LongParameter.builder()
            .name("TARGET_RUN_DATE")
            .value(1000L)
            .evaluatedResult(1000L)
            .evaluatedTime(123L)
            .mode(ParamMode.MUTABLE_ON_START)
            .build());
    workflowInstance.setParams(instanceParams);

    Initiator.Type[] initiators =
        new Initiator.Type[] {
          Initiator.Type.FOREACH,
          Initiator.Type.WHILE,
          Initiator.Type.SUBWORKFLOW,
          Initiator.Type.TEMPLATE
        };
    for (Initiator.Type initiator : initiators) {
      RunRequest request =
          RunRequest.builder()
              .initiator(UpstreamInitiator.withType(initiator))
              .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
              .restartConfig(
                  RestartConfig.builder()
                      .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
                      .restartPath(
                          Collections.singletonList(
                              new RestartConfig.RestartNode(
                                  workflowSummary.getWorkflowId(),
                                  workflowSummary.getWorkflowInstanceId(),
                                  "step1")))
                      .restartParams(restartParams)
                      .build())
              .build();
      Map<String, Parameter> params =
          paramsManager.generateMergedWorkflowParams(workflowInstance, request);
      Assert.assertEquals(
          Long.valueOf(1001L), params.get("TARGET_RUN_DATE").asLongParam().getValue());
    }
  }

  @Test
  public void testWorkflowParamRunParamsStartRestart() {
    Map<String, ParamDefinition> runParams =
        singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1"));
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .runParams(runParams)
            .build();
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);
    Assert.assertFalse(workflowParams.isEmpty());
    Assert.assertEquals("d1", workflowParams.get("p1").asStringParam().getValue());
    Assert.assertEquals(ParamSource.LAUNCH, workflowParams.get("p1").getSource());

    // restart should propagate to source
    request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .runParams(runParams)
            .build();

    Map<String, Parameter> instanceParams = new LinkedHashMap<>();
    instanceParams.put("RUN_TS", buildParam("RUN_TS", 123L));
    instanceParams.put(
        "DSL_DEFAULT_TZ",
        StringParameter.builder()
            .name("DSL_DEFAULT_TZ")
            .value("US/Pacific")
            .evaluatedResult("US/Pacific")
            .evaluatedTime(123L)
            .mode(ParamMode.MUTABLE_ON_START)
            .build());
    workflowInstance.setParams(instanceParams);
    Map<String, Parameter> restartWorkflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);
    Assert.assertEquals("d1", restartWorkflowParams.get("p1").asStringParam().getValue());
    Assert.assertEquals(ParamSource.RESTART, restartWorkflowParams.get("p1").getSource());
    Assert.assertEquals(
        Long.valueOf(123), restartWorkflowParams.get("RUN_TS").asLongParam().getValue());
    Assert.assertEquals(
        "US/Pacific", restartWorkflowParams.get("DSL_DEFAULT_TZ").asStringParam().getValue());

    // should fail the restart
    runParams =
        singletonMap(
            "DSL_DEFAULT_TZ", ParamDefinition.buildParamDefinition("DSL_DEFAULT_TZ", "UTC"));
    RunRequest badRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .runParams(runParams)
            .build();
    AssertHelper.assertThrows(
        "Cannot modify param with MUTABLE_ON_START during restart",
        MaestroValidationException.class,
        "Cannot modify param with mode [MUTABLE_ON_START] for parameter [DSL_DEFAULT_TZ]",
        () -> paramsManager.generateMergedWorkflowParams(workflowInstance, badRequest));
  }

  @Test
  public void testGetSignalParamsEmpty() {
    Assert.assertTrue(ParamsManager.getSignalDependenciesParameters(null).isEmpty());
    Assert.assertTrue(ParamsManager.getSignalOutputsParameters(null).isEmpty());
  }

  @Test
  public void testCalculateTimezonesNoTriggers() throws IOException {
    DefaultParamManager defaultsManager = new DefaultParamManager(yamlMapper);
    defaultsManager.init();
    paramsManager = new ParamsManager(defaultsManager);
    Step step = Mockito.mock(Step.class);
    when(step.getType()).thenReturn(StepType.TITUS);
    RunProperties runProperties = new RunProperties();
    runProperties.setOwner(User.builder().name("demo").build());
    workflowInstance.setRunProperties(runProperties);
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);

    paramExtensionRepo.reset(
        Collections.emptyMap(), null, InstanceWrapper.from(workflowInstance, request));
    paramEvaluator.evaluateWorkflowParameters(workflowParams, workflow.getId());
    paramExtensionRepo.clear();

    // if no triggers, default to UTC
    Assert.assertEquals("UTC", workflowParams.get("WORKFLOW_CRON_TIMEZONE").asString());
    Assert.assertEquals("UTC", workflowParams.get("DSL_DEFAULT_TZ").asString());
    Assert.assertEquals("demo", workflowParams.get("owner").asString());
  }

  @Test
  public void testCalculateTimezonesWithTriggers() throws IOException {
    WorkflowDefinition definition =
        loadObject(
            "fixtures/parameters/sample-wf-with-time-triggers.json", WorkflowDefinition.class);
    workflow = definition.getWorkflow();
    workflowInstance.setRuntimeWorkflow(workflow);
    paramsManager = new ParamsManager(defaultsManager);
    Step step = Mockito.mock(Step.class);
    when(step.getType()).thenReturn(StepType.TITUS);
    RunProperties runProperties = new RunProperties();
    runProperties.setOwner(User.builder().name("demo").build());
    workflowInstance.setRunProperties(runProperties);
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();

    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);

    paramExtensionRepo.reset(
        Collections.emptyMap(), null, InstanceWrapper.from(workflowInstance, request));
    paramEvaluator.evaluateWorkflowParameters(workflowParams, workflow.getId());
    paramExtensionRepo.clear();

    // if triggers, use first trigger timezone
    Assert.assertEquals("US/Pacific", workflowParams.get("WORKFLOW_CRON_TIMEZONE").asString());
    Assert.assertEquals("US/Pacific", workflowParams.get("DSL_DEFAULT_TZ").asString());
    Assert.assertEquals("demo", workflowParams.get("owner").asString());
  }

  @Test
  public void testCalculateTimezonesWithTriggersAndTimeInitiator() throws IOException {
    WorkflowDefinition definition =
        loadObject(
            "fixtures/parameters/sample-wf-with-time-triggers.json", WorkflowDefinition.class);
    workflow = definition.getWorkflow();
    workflowInstance.setRuntimeWorkflow(workflow);
    paramsManager = new ParamsManager(defaultsManager);
    Step step = Mockito.mock(Step.class);
    when(step.getType()).thenReturn(StepType.TITUS);
    RunProperties runProperties = new RunProperties();
    runProperties.setOwner(User.builder().name("demo").build());
    workflowInstance.setRunProperties(runProperties);
    TimeInitiator initiator = new TimeInitiator();
    initiator.setTimezone("Asia/Tokyo");
    RunRequest request =
        RunRequest.builder()
            .initiator(initiator)
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);

    paramExtensionRepo.reset(
        Collections.emptyMap(), null, InstanceWrapper.from(workflowInstance, request));
    paramEvaluator.evaluateWorkflowParameters(workflowParams, workflow.getId());
    paramExtensionRepo.clear();

    // if triggers and initiator both, use first trigger timezone for default tz, and initiator for
    // the workflow tz
    Assert.assertEquals("Asia/Tokyo", workflowParams.get("WORKFLOW_CRON_TIMEZONE").asString());
    Assert.assertEquals("US/Pacific", workflowParams.get("DSL_DEFAULT_TZ").asString());
    Assert.assertEquals("demo", workflowParams.get("owner").asString());
  }

  @Test
  public void testStaticWorkflowParamMerge() {
    workflow.getParams().put("p1", ParamDefinition.buildParamDefinition("p1", "d1"));
    when(defaultParamManager.getDefaultWorkflowParams())
        .thenReturn(singletonMap("p2", ParamDefinition.buildParamDefinition("p2", "d2")));
    Map<String, ParamDefinition> mergedWorkflowParamDefs =
        paramsManager.generatedStaticWorkflowParamDefs(workflow);
    Assert.assertEquals("d1", mergedWorkflowParamDefs.get("p1").asStringParamDef().getValue());
    Assert.assertEquals("d2", mergedWorkflowParamDefs.get("p2").asStringParamDef().getValue());
  }

  @Test
  public void testStaticStepParamMerge() {
    Map<String, ParamDefinition> stepParams = new LinkedHashMap<>();
    when(defaultParamManager.getDefaultStepParams())
        .thenReturn(singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));
    when(stepRuntime.injectRuntimeParams(any(), any()))
        .thenReturn(singletonMap("p2", ParamDefinition.buildParamDefinition("p2", "d2")));
    when(defaultParamManager.getDefaultParamsForType(any()))
        .thenReturn(
            Optional.of(singletonMap("p3", ParamDefinition.buildParamDefinition("p3", "d3"))));
    ((TypedStep) step).setParams(stepParams);
    stepParams.put("p4", ParamDefinition.buildParamDefinition("p4", "d4"));
    Map<String, ParamDefinition> mergedStepParamDefs =
        paramsManager.generateStaticStepParamDefs(workflowSummary, step, stepRuntime);

    Assert.assertEquals("d1", mergedStepParamDefs.get("p1").asStringParamDef().getValue());
    Assert.assertEquals("d2", mergedStepParamDefs.get("p2").asStringParamDef().getValue());
    Assert.assertEquals("d3", mergedStepParamDefs.get("p3").asStringParamDef().getValue());
    Assert.assertEquals("d4", mergedStepParamDefs.get("p4").asStringParamDef().getValue());
  }

  @Test
  public void testRestartConfigRunParamMerge() {
    Map<String, ParamDefinition> runParams =
        singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1"));
    Map<String, ParamDefinition> restartParams =
        singletonMap("p2", ParamDefinition.buildParamDefinition("p2", "d2"));
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .runParams(runParams)
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode("sample-wf-map-params", 1, "foo")
                    .restartParams(restartParams)
                    .build())
            .build();
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);
    Assert.assertFalse(workflowParams.isEmpty());
    Assert.assertEquals("d1", workflowParams.get("p1").asStringParam().getValue());
    Assert.assertEquals("d2", workflowParams.get("p2").asStringParam().getValue());
    Assert.assertEquals(ParamSource.RESTART, workflowParams.get("p1").getSource());
    Assert.assertEquals(ParamSource.RESTART, workflowParams.get("p2").getSource());
  }

  @Test
  public void testRestartConfigRunUnchangedParamMerge() {
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .restartConfig(
                RestartConfig.builder().addRestartNode("sample-wf-map-params", 1, "foo").build())
            .build();
    Map<String, Object> meta =
        Collections.singletonMap(Constants.METADATA_SOURCE_KEY, "SYSTEM_DEFAULT");
    Map<String, Parameter> instanceParams = new LinkedHashMap<>();
    instanceParams.put(
        "TARGET_RUN_DATE",
        LongParameter.builder()
            .name("TARGET_RUN_DATE")
            .value(1000L)
            .evaluatedResult(1000L)
            .evaluatedTime(123L)
            .mode(ParamMode.MUTABLE_ON_START)
            .meta(meta)
            .build());
    workflowInstance.setParams(instanceParams);
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);
    Assert.assertFalse(workflowParams.isEmpty());
    Assert.assertEquals(
        ParamSource.SYSTEM_DEFAULT, workflowParams.get("TARGET_RUN_DATE").getSource());
  }

  @Test
  public void testRestartConfigRunChangedParamMerge() {
    Map<String, Object> meta =
        Collections.singletonMap(Constants.METADATA_SOURCE_KEY, "SYSTEM_DEFAULT");
    LongParameter param =
        LongParameter.builder()
            .name("TARGET_RUN_DATE")
            .value(1000L)
            .evaluatedResult(1000L)
            .evaluatedTime(123L)
            .mode(ParamMode.MUTABLE_ON_START_RESTART)
            .meta(meta)
            .build();
    Map<String, ParamDefinition> restartParams =
        singletonMap(
            "TARGET_RUN_DATE",
            LongParamDefinition.builder()
                .name("TARGET_RUN_DATE")
                .value(1001L)
                .mode(ParamMode.MUTABLE_ON_START_RESTART)
                .build());
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode("sample-wf-map-params", 1, "foo")
                    .restartParams(restartParams)
                    .build())
            .build();
    Map<String, Parameter> instanceParams = new LinkedHashMap<>();
    instanceParams.put("TARGET_RUN_DATE", param);
    workflowInstance.setParams(instanceParams);
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);
    Assert.assertFalse(workflowParams.isEmpty());
    Assert.assertEquals(
        Long.valueOf(1001L), workflowParams.get("TARGET_RUN_DATE").asLongParam().getValue());
    Assert.assertEquals(ParamSource.RESTART, workflowParams.get("TARGET_RUN_DATE").getSource());
  }

  @Test
  public void testSubRestartConfigRunUnchangedParamMerge() {
    Map<String, Object> meta =
        Collections.singletonMap(Constants.METADATA_SOURCE_KEY, "SUBWORKFLOW");
    LongParameter param =
        LongParameter.builder()
            .name("TARGET_RUN_DATE")
            .value(1000L)
            .evaluatedResult(1000L)
            .evaluatedTime(123L)
            .mode(ParamMode.MUTABLE_ON_START)
            .meta(meta)
            .build();
    Map<String, Object> restartMeta =
        Collections.singletonMap(Constants.METADATA_SOURCE_KEY, "DEFINITION");
    Map<String, ParamDefinition> restartParams =
        singletonMap(
            "TARGET_RUN_DATE",
            param.toDefinition().asLongParamDef().toBuilder()
                .mode(ParamMode.MUTABLE)
                .meta(restartMeta)
                .build());
    RunRequest request =
        RunRequest.builder()
            .initiator(new SubworkflowInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .runParams(restartParams)
            .restartConfig(
                RestartConfig.builder().addRestartNode("sample-wf-map-params", 1, "foo").build())
            .build();
    Map<String, Parameter> instanceParams = new LinkedHashMap<>();
    instanceParams.put("TARGET_RUN_DATE", param);
    workflowInstance.setParams(instanceParams);
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);
    Assert.assertFalse(workflowParams.isEmpty());
    Assert.assertEquals(
        Long.valueOf(1000L), workflowParams.get("TARGET_RUN_DATE").asLongParam().getValue());
    Assert.assertEquals(ParamSource.SUBWORKFLOW, workflowParams.get("TARGET_RUN_DATE").getSource());
    Assert.assertEquals(
        ParamMode.MUTABLE_ON_START, workflowParams.get("TARGET_RUN_DATE").getMode());
  }

  @Test
  public void testSubRestartConfigRunChangedParamMerge() {
    Map<String, Object> meta =
        Collections.singletonMap(Constants.METADATA_SOURCE_KEY, "SUBWORKFLOW");
    LongParameter param =
        LongParameter.builder()
            .name("TARGET_RUN_DATE")
            .value(1000L)
            .evaluatedResult(1000L)
            .evaluatedTime(123L)
            .mode(ParamMode.MUTABLE_ON_START_RESTART)
            .meta(meta)
            .build();
    Map<String, Object> restartMeta =
        Collections.singletonMap(Constants.METADATA_SOURCE_KEY, "DEFINITION");
    Map<String, ParamDefinition> restartParams =
        singletonMap(
            "TARGET_RUN_DATE",
            param.toDefinition().asLongParamDef().toBuilder()
                .value(1001L)
                .mode(ParamMode.MUTABLE)
                .meta(restartMeta)
                .build());
    RunRequest request =
        RunRequest.builder()
            .initiator(new SubworkflowInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .runParams(restartParams)
            .restartConfig(
                RestartConfig.builder().addRestartNode("sample-wf-map-params", 1, "foo").build())
            .build();
    Map<String, Parameter> instanceParams = new LinkedHashMap<>();
    instanceParams.put("TARGET_RUN_DATE", param);
    workflowInstance.setParams(instanceParams);
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);
    Assert.assertFalse(workflowParams.isEmpty());
    Assert.assertEquals(
        Long.valueOf(1001L), workflowParams.get("TARGET_RUN_DATE").asLongParam().getValue());
    Assert.assertEquals(ParamSource.SUBWORKFLOW, workflowParams.get("TARGET_RUN_DATE").getSource());
    Assert.assertEquals(
        ParamMode.MUTABLE_ON_START_RESTART, workflowParams.get("TARGET_RUN_DATE").getMode());
  }

  @Test
  public void testRestartConfigStepRunParamMerge() {
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        singletonMap(
            "stepid", singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));

    Map<String, Map<String, ParamDefinition>> stepRestartParams =
        singletonMap(
            "stepid", singletonMap("p2", ParamDefinition.buildParamDefinition("p2", "d2")));

    ManualInitiator manualInitiator = new ManualInitiator();
    workflowSummary.setInitiator(manualInitiator);
    workflowSummary.setStepRunParams(stepRunParams);
    workflowSummary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("sample-wf-map-params", 1, "foo")
            .stepRestartParams(stepRestartParams)
            .build());

    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);
    Assert.assertEquals("d1", stepParams.get("p1").asStringParam().getValue());
    Assert.assertEquals("d2", stepParams.get("p2").asStringParam().getValue());
    Assert.assertEquals(ParamSource.RESTART, stepParams.get("p1").getSource());
    Assert.assertEquals(ParamSource.RESTART, stepParams.get("p2").getSource());
    Assert.assertEquals(
        Arrays.asList("p2", "p1"),
        new ArrayList<>(stepParams.keySet()).subList(stepParams.size() - 2, stepParams.size()));
  }

  /**
   * This test verifies that restarting of foreach step fails because the "loop_params" is mutated.
   *
   * @throws IOException io exception
   */
  @Test
  public void testRestartForeachStepRunParamMerge() throws IOException {
    DefaultParamManager defaultParamManager =
        new DefaultParamManager(JsonHelper.objectMapperWithYaml());
    defaultParamManager.init();
    ParamsManager paramsManager = new ParamsManager(defaultParamManager);
    Map<String, ParamDefinition> loopParamsDef = new HashMap<>();
    loopParamsDef.put("a", ParamDefinition.buildParamDefinition("i", new long[] {1, 2}));
    Map<String, ParamDefinition> loopParamsRestart = new HashMap<>();
    loopParamsRestart.put("a", ParamDefinition.buildParamDefinition("i", new long[] {1, 2, 3}));
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        singletonMap(
            "stepid",
            singletonMap(
                "loop_params",
                MapParamDefinition.builder().name("loop_params").value(loopParamsDef).build()));

    Map<String, Map<String, ParamDefinition>> stepRestartParams =
        singletonMap(
            "stepid",
            singletonMap(
                "loop_params",
                MapParamDefinition.builder().name("loop_params").value(loopParamsRestart).build()));

    ManualInitiator manualInitiator = new ManualInitiator();
    workflowSummary.setInitiator(manualInitiator);
    workflowSummary.setStepRunParams(stepRunParams);
    workflowSummary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("sample-wf-map-params", 1, "stepid")
            .stepRestartParams(stepRestartParams)
            .build());

    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    Step step = Mockito.mock(Step.class);
    when(step.getType()).thenReturn(StepType.FOREACH);
    when(step.getParams()).thenReturn(null);
    AssertHelper.assertThrows(
        "Cannot modify param with MUTABLE_ON_START during restart",
        MaestroValidationException.class,
        "Cannot modify param with mode [MUTABLE_ON_START] for parameter [a]",
        () ->
            paramsManager.generateMergedStepParams(
                workflowSummary, step, stepRuntime, runtimeSummary));
  }

  /**
   * This test verifies that restarting of subworkflow step fails because the "subworkflow_id",
   * "subworkflow_version" are mutated.
   *
   * @throws IOException io exception
   */
  @Test
  public void testRestartSubworkflowStepRunParamMerge() throws IOException {
    DefaultParamManager defaultParamManager =
        new DefaultParamManager(JsonHelper.objectMapperWithYaml());
    defaultParamManager.init();
    ParamsManager paramsManager = new ParamsManager(defaultParamManager);
    for (String paramName : new String[] {"subworkflow_id", "subworkflow_version"}) {
      Map<String, Map<String, ParamDefinition>> stepRunParams =
          singletonMap(
              "stepid",
              singletonMap(paramName, ParamDefinition.buildParamDefinition(paramName, "1")));

      Map<String, Map<String, ParamDefinition>> stepRestartParams =
          singletonMap(
              "stepid",
              singletonMap(paramName, ParamDefinition.buildParamDefinition(paramName, "2")));

      ManualInitiator manualInitiator = new ManualInitiator();
      workflowSummary.setInitiator(manualInitiator);
      workflowSummary.setStepRunParams(stepRunParams);
      workflowSummary.setRestartConfig(
          RestartConfig.builder()
              .addRestartNode("sample-wf-map-params", 1, "stepid")
              .stepRestartParams(stepRestartParams)
              .build());

      workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
      Step step = Mockito.mock(Step.class);
      when(step.getType()).thenReturn(StepType.SUBWORKFLOW);
      when(step.getParams()).thenReturn(null);
      AssertHelper.assertThrows(
          "Cannot modify param with MUTABLE_ON_START during restart",
          MaestroValidationException.class,
          "Cannot modify param with mode [MUTABLE_ON_START] for parameter [" + paramName + "]",
          () ->
              paramsManager.generateMergedStepParams(
                  workflowSummary, step, stepRuntime, runtimeSummary));
    }
  }

  @Test
  public void testRestartConfigStepRunParamMergeOrder() {
    ((TypedStep) step)
        .setParams(
            twoItemMap(
                "p1", ParamDefinition.buildParamDefinition("p1", "d1"),
                "p2", ParamDefinition.buildParamDefinition("p2", "d2")));

    Map<String, Map<String, ParamDefinition>> stepRunParams =
        singletonMap(
            "stepid",
            twoItemMap(
                "p2", ParamDefinition.buildParamDefinition("p2", "d3"),
                "p3", ParamDefinition.buildParamDefinition("p3", "d4")));

    Map<String, Map<String, ParamDefinition>> stepRestartParams =
        singletonMap(
            "stepid",
            twoItemMap(
                "p2", ParamDefinition.buildParamDefinition("p2", "d5"),
                "pp", ParamDefinition.buildParamDefinition("pp", "dd")));

    ManualInitiator manualInitiator = new ManualInitiator();
    workflowSummary.setInitiator(manualInitiator);
    workflowSummary.setStepRunParams(stepRunParams);
    workflowSummary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("sample-wf-map-params", 1, "foo")
            .stepRestartParams(stepRestartParams)
            .build());

    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    Map<String, Parameter> stepParams =
        paramsManager.generateMergedStepParams(workflowSummary, step, stepRuntime, runtimeSummary);
    Assert.assertEquals("d1", stepParams.get("p1").asStringParam().getValue());
    Assert.assertEquals("d5", stepParams.get("p2").asStringParam().getValue());
    Assert.assertEquals("d4", stepParams.get("p3").asStringParam().getValue());
    Assert.assertEquals("dd", stepParams.get("pp").asStringParam().getValue());
    Assert.assertEquals(ParamSource.DEFINITION, stepParams.get("p1").getSource());
    Assert.assertEquals(ParamSource.RESTART, stepParams.get("p2").getSource());
    Assert.assertEquals(ParamSource.RESTART, stepParams.get("p3").getSource());
    Assert.assertEquals(ParamSource.RESTART, stepParams.get("pp").getSource());
    Assert.assertEquals(
        Arrays.asList("pp", "p3", "p1", "p2"),
        new ArrayList<>(stepParams.keySet()).subList(stepParams.size() - 4, stepParams.size()));
  }

  @Test
  public void testCalculateUserDefinedSelParams() {
    paramsManager = new ParamsManager(defaultsManager);
    Step step = Mockito.mock(Step.class);
    when(step.getType()).thenReturn(StepType.TITUS);
    RunProperties runProperties = new RunProperties();
    runProperties.setOwner(User.builder().name("demo").build());
    workflowInstance.setRunProperties(runProperties);
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .runParams(
                Collections.singletonMap(
                    "workflow_run_policy",
                    StringParamDefinition.builder()
                        .expression("return params.getFromInstance('RUN_POLICY');")
                        .build()))
            .build();
    Map<String, Parameter> workflowParams =
        paramsManager.generateMergedWorkflowParams(workflowInstance, request);

    paramExtensionRepo.reset(
        Collections.emptyMap(), null, InstanceWrapper.from(workflowInstance, request));
    paramEvaluator.evaluateWorkflowParameters(workflowParams, workflow.getId());
    paramExtensionRepo.clear();

    Assert.assertEquals("UTC", workflowParams.get("WORKFLOW_CRON_TIMEZONE").asString());
    Assert.assertEquals("UTC", workflowParams.get("DSL_DEFAULT_TZ").asString());
    Assert.assertEquals("demo", workflowParams.get("owner").asString());
    // get run policy
    Assert.assertEquals(
        "START_FRESH_NEW_RUN", workflowParams.get("workflow_run_policy").asString());
  }
}
