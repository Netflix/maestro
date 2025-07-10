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
package com.netflix.maestro.engine.eval;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.handlers.SignalHandler;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.initiator.SignalInitiator;
import com.netflix.maestro.models.initiator.SubworkflowInitiator;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.StringMapParameter;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalParamValue;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class MaestroParamExtensionTest extends MaestroEngineBaseTest {
  private static final String TEST_STEP_RUNTIME_SUMMARY =
      "fixtures/execution/sample-step-runtime-summary-2.json";
  private static final String TEST_SUBWORKFLOW_STEP_RUNTIME_SUMMARY =
      "fixtures/execution/sample-step-runtime-summary-1.json";
  private static final String TEST_STEP_INSTANCE =
      "fixtures/instances/sample-step-instance-succeeded.json";

  @Mock MaestroStepInstanceDao stepInstanceDao;
  @Mock InstanceWrapper instanceWrapper;
  @Mock Map<String, Map<String, Object>> allStepOutputData;
  @Mock SignalHandler handler;
  MaestroParamExtension paramExtension;

  @Before
  public void before() throws Exception {
    paramExtension =
        new MaestroParamExtension(
            Executors.newSingleThreadExecutor(),
            stepInstanceDao,
            "prod",
            allStepOutputData,
            handler,
            instanceWrapper,
            MAPPER);
  }

  @Test
  public void testGetFromStep() throws Exception {
    StepRuntimeSummary summary = loadObject(TEST_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    when(allStepOutputData.get("step1"))
        .thenReturn(Collections.singletonMap("maestro_step_runtime_summary", summary));
    assertEquals("foo", paramExtension.getFromStep("step1", "param1"));
    assertEquals("SUCCEEDED", paramExtension.getFromStep("step1", Constants.STEP_STATUS_PARAM));
    assertEquals(
        1608171805401L, paramExtension.getFromStep("step1", Constants.STEP_END_TIME_PARAM));
  }

  @Test
  public void testGetFromSignalInitiator() {
    SignalInitiator initiator = Mockito.mock(SignalInitiator.class);
    when(instanceWrapper.getInitiator()).thenReturn(initiator);
    when(initiator.getType()).thenReturn(Initiator.Type.SIGNAL);
    when(initiator.getParams())
        .thenReturn(
            twoItemMap(
                "param1",
                StringParameter.builder().evaluatedResult("value1").build(),
                "param2",
                LongParameter.builder().evaluatedResult(123L).build()));
    assertEquals("value1", paramExtension.getFromSignal("param1"));
    assertEquals(123L, paramExtension.getFromSignal("param2"));
  }

  @Test
  public void testGetFromSignal() {
    SignalInitiator initiator = Mockito.mock(SignalInitiator.class);
    when(instanceWrapper.getInitiator()).thenReturn(initiator);
    when(initiator.getType()).thenReturn(Initiator.Type.SIGNAL);
    when(initiator.getSignalIdMap()).thenReturn(Map.of("signal-a", 12L, "signal-b", 56L));
    SignalInstance instance1 = new SignalInstance();
    instance1.setParams(Collections.singletonMap("param1", SignalParamValue.of("value1")));
    when(handler.getSignalInstance("signal-a", 12)).thenReturn(instance1);
    SignalInstance instance2 = new SignalInstance();
    instance2.setParams(Collections.singletonMap("param2", SignalParamValue.of(123L)));
    when(handler.getSignalInstance("signal-b", 56)).thenReturn(instance2);

    assertEquals("value1", paramExtension.getFromSignal("signal-a", "param1"));
    assertEquals(123L, paramExtension.getFromSignal("signal-b", "param2"));
  }

  @Test
  public void testGetFromSignalOrDefault() {
    SignalInitiator initiator = Mockito.mock(SignalInitiator.class);
    when(instanceWrapper.getInitiator()).thenReturn(initiator);
    when(initiator.getType()).thenReturn(Initiator.Type.SIGNAL);
    when(initiator.getSignalIdMap()).thenReturn(Map.of("signal-a", 12L, "signal-b", 56L));
    SignalInstance instance1 = new SignalInstance();
    instance1.setParams(Collections.singletonMap("param1", SignalParamValue.of("value1")));
    when(handler.getSignalInstance("signal-a", 12)).thenReturn(instance1);

    assertEquals("value1", paramExtension.getFromSignalOrDefault("signal-a", "param1", "value2"));
    assertEquals(
        "defaultValue",
        paramExtension.getFromSignalOrDefault("signal-a", "nonExistingParam", "defaultValue"));
    assertEquals(
        "defaultValue",
        paramExtension.getFromSignalOrDefault(
            "signal-non-existing", "nonExistingParam", "defaultValue"));
  }

  @Test
  public void testGetFromForeach() throws Exception {
    StepRuntimeSummary summary = loadObject(TEST_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    when(allStepOutputData.get("foreach-job"))
        .thenReturn(Collections.singletonMap("maestro_step_runtime_summary", summary));
    when(stepInstanceDao.getForeachParamType(any(), any(), any())).thenReturn(ParamType.LONG);
    when(stepInstanceDao.getEvaluatedResultsFromForeach(any(), any(), any()))
        .thenReturn(Collections.singletonMap(1L, "12"));
    long[] res = (long[]) paramExtension.getFromForeach("foreach-job", "job1", "sleep_seconds");
    assertArrayEquals(new long[] {12, 0, 0, 0, 0, 0}, res);
  }

  @Test
  public void testGetFromSubworkflow() throws Exception {
    StepRuntimeSummary summary =
        loadObject(TEST_SUBWORKFLOW_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    when(allStepOutputData.get("foo"))
        .thenReturn(Collections.singletonMap("maestro_step_runtime_summary", summary));
    StepInstance stepInSubworkflow = loadObject(TEST_STEP_INSTANCE, StepInstance.class);
    when(stepInstanceDao.getStepInstanceView(any(), anyLong(), any()))
        .thenReturn(stepInSubworkflow);
    long res = (Long) paramExtension.getFromSubworkflow("foo", "job1", "sleep_seconds");
    assertEquals(15, res);
  }

  @Test
  public void testInvalidGetFromStep() throws Exception {
    StepRuntimeSummary summary = loadObject(TEST_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    when(allStepOutputData.get("step1"))
        .thenReturn(Collections.singletonMap("maestro_step_runtime_summary", summary));

    AssertHelper.assertThrows(
        "Cannot find the referenced step id",
        MaestroInternalError.class,
        "getFromStep throws an exception",
        () -> paramExtension.getFromStep("step2", "param1"));

    AssertHelper.assertThrows(
        "Cannot find the referenced param name",
        MaestroInternalError.class,
        "getFromStep throws an exception",
        () -> paramExtension.getFromStep("step1", "param2"));

    summary.getParams().get("param1").setEvaluatedTime(null);

    AssertHelper.assertThrows(
        "Referenced param is not evaluated yet.",
        MaestroInternalError.class,
        "getFromStep throws an exception",
        () -> paramExtension.getFromStep("step1", "param1"));
  }

  @Test
  public void testInvalidGetFromSignal() {
    SignalInitiator initiator = Mockito.mock(SignalInitiator.class);
    when(instanceWrapper.getInitiator()).thenReturn(initiator);

    when(initiator.getType()).thenReturn(Initiator.Type.TIME);
    AssertHelper.assertThrows(
        "Cannot get a param from non signal initiator",
        MaestroInternalError.class,
        "getFromSignal throws an exception",
        () -> paramExtension.getFromSignal("signal-a", "param1"));

    when(initiator.getType()).thenReturn(Initiator.Type.SIGNAL);

    AssertHelper.assertThrows(
        "signal initiator's param is null",
        MaestroInternalError.class,
        "getFromSignal throws an exception",
        () -> paramExtension.getFromSignal("signal-a", "param1"));

    when(initiator.getParams()).thenReturn(Collections.emptyMap());

    AssertHelper.assertThrows(
        "Cannot find signal",
        MaestroInternalError.class,
        "getFromSignal throws an exception",
        () -> paramExtension.getFromSignal("signal-not-existing", "param1"));

    when(initiator.getParams())
        .thenReturn(
            singletonMap("signal-a", StringParameter.builder().evaluatedResult("foo").build()));

    AssertHelper.assertThrows(
        "Invalid param type, which must be MAP or STRING_MAP",
        MaestroInternalError.class,
        "getFromSignal throws an exception",
        () -> paramExtension.getFromSignal("signal-a", "param1"));

    when(initiator.getParams())
        .thenReturn(
            twoItemMap(
                "signal-a",
                StringMapParameter.builder()
                    .evaluatedResult(singletonMap("param1", "value1"))
                    .build(),
                "signal-b",
                MapParameter.builder().evaluatedResult(singletonMap("param2", 123L)).build()));

    AssertHelper.assertThrows(
        "Cannot find param from the signal",
        MaestroInternalError.class,
        "getFromSignal throws an exception",
        () -> paramExtension.getFromSignal("signal-a", "param-not-existing"));

    AssertHelper.assertThrows(
        "Cannot find string param from the signal",
        MaestroInternalError.class,
        "getFromSignal throws an exception",
        () -> paramExtension.getFromSignal("signal-b", "param-not-existing"));
  }

  @Test
  public void testInvalidGetFromForeach() throws Exception {
    AssertHelper.assertThrows(
        "Cannot find the referenced step id",
        MaestroInternalError.class,
        "getFromForeach throws an exception",
        () -> paramExtension.getFromForeach("non-existing-job", "job1", "sleep_seconds"));

    StepRuntimeSummary summary = StepRuntimeSummary.builder().type(StepType.NOOP).build();

    when(allStepOutputData.get("foreach-job"))
        .thenReturn(Collections.singletonMap("maestro_step_runtime_summary", summary));
    AssertHelper.assertThrows(
        "Only foreach step can call getFromForeach",
        MaestroInternalError.class,
        "getFromForeach throws an exception",
        () -> paramExtension.getFromForeach("foreach-job", "job1", "sleep_seconds"));

    summary = loadObject(TEST_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    summary.getArtifacts().remove(Artifact.Type.FOREACH.key());
    when(allStepOutputData.get("foreach-job"))
        .thenReturn(Collections.singletonMap("maestro_step_runtime_summary", summary));
    AssertHelper.assertThrows(
        "Cannot load param from uninitialized foreach",
        MaestroInternalError.class,
        "getFromForeach throws an exception",
        () -> paramExtension.getFromForeach("foreach-job", "job1", "sleep_seconds"));

    summary = loadObject(TEST_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    when(allStepOutputData.get("foreach-job"))
        .thenReturn(Collections.singletonMap("maestro_step_runtime_summary", summary));
    when(stepInstanceDao.getForeachParamType(any(), any(), any())).thenReturn(ParamType.LONG_ARRAY);
    when(stepInstanceDao.getEvaluatedResultsFromForeach(any(), any(), any()))
        .thenReturn(Collections.singletonMap(1L, "12"));

    AssertHelper.assertThrows(
        "cannot get non-primitive type param from foreach",
        MaestroInternalError.class,
        "getFromForeach throws an exception",
        () -> paramExtension.getFromForeach("foreach-job", "job1", "sleep_seconds"));
  }

  @Test
  public void testInvalidGetFromSubworkflow() throws Exception {
    AssertHelper.assertThrows(
        "Cannot find the referenced step id",
        MaestroInternalError.class,
        "getFromSubworkflow throws an exception",
        () -> paramExtension.getFromSubworkflow("non-existing-job", "job1", "sleep_seconds"));

    StepRuntimeSummary summary = loadObject(TEST_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    when(allStepOutputData.get("foo"))
        .thenReturn(Collections.singletonMap("maestro_step_runtime_summary", summary));
    AssertHelper.assertThrows(
        "step type is not subworkflow",
        MaestroInternalError.class,
        "getFromSubworkflow throws an exception",
        () -> paramExtension.getFromSubworkflow("foo", "job1", "sleep_seconds"));

    summary = loadObject(TEST_SUBWORKFLOW_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    when(allStepOutputData.get("foo"))
        .thenReturn(Collections.singletonMap("maestro_step_runtime_summary", summary));
    StepInstance stepInSubworkflow = loadObject(TEST_STEP_INSTANCE, StepInstance.class);
    when(stepInstanceDao.getStepInstance(any(), anyLong(), anyLong(), any(), any()))
        .thenReturn(stepInSubworkflow);
    AssertHelper.assertThrows(
        "param name does not exist",
        MaestroInternalError.class,
        "getFromSubworkflow throws an exception",
        () -> paramExtension.getFromSubworkflow("foo", "job1", "not-existing"));
  }

  @Test
  public void testNextUniqueId() {
    Long expected = 750762533885116445L;
    when(stepInstanceDao.getNextUniqueId()).thenReturn(expected);
    assertEquals(expected, paramExtension.nextUniqueId());

    when(stepInstanceDao.getNextUniqueId())
        .thenThrow(new MaestroNotFoundException("test exception"));
    AssertHelper.assertThrows(
        "cannot get next unique id",
        MaestroInternalError.class,
        "nextUniqueId throws an exception",
        () -> paramExtension.nextUniqueId());
  }

  @Test
  public void testGetFromInstance() {
    when(instanceWrapper.getWorkflowId()).thenReturn("test-workflow-id");
    when(instanceWrapper.getWorkflowInstanceId()).thenReturn(2L);
    when(instanceWrapper.getWorkflowRunId()).thenReturn(3L);
    when(instanceWrapper.isWorkflowParam()).thenReturn(true);
    when(instanceWrapper.getInitiatorTimeZone()).thenReturn("US/Pacific");
    Initiator initiator = new ManualInitiator();
    initiator.setCaller(User.create("tester"));
    when(instanceWrapper.getInitiator()).thenReturn(initiator);
    when(instanceWrapper.getRunPolicy()).thenReturn("START_FRESH_NEW_RUN");
    when(instanceWrapper.getWorkflowOwner()).thenReturn("tester");
    when(instanceWrapper.getFirstTimeTriggerTimeZone()).thenReturn("UTC");
    assertEquals("US/Pacific", paramExtension.getFromInstance(Constants.INITIATOR_TIMEZONE_PARAM));
    assertEquals("MANUAL", paramExtension.getFromInstance(Constants.INITIATOR_TYPE_PARAM));
    assertEquals("tester", paramExtension.getFromInstance(Constants.INITIATOR_RUNNER_NAME));
    assertEquals(
        "START_FRESH_NEW_RUN", paramExtension.getFromInstance(Constants.WORKFLOW_RUN_POLICY_PARAM));
    assertEquals("tester", paramExtension.getFromInstance(Constants.WORKFLOW_OWNER_PARAM));
    assertEquals(
        "UTC", paramExtension.getFromInstance(Constants.FIRST_TIME_TRIGGER_TIMEZONE_PARAM));
    assertEquals("test-workflow-id", paramExtension.getFromInstance(Constants.WORKFLOW_ID_PARAM));
    assertEquals(2L, paramExtension.getFromInstance(Constants.WORKFLOW_INSTANCE_ID_PARAM));
    assertEquals(3L, paramExtension.getFromInstance(Constants.WORKFLOW_RUN_ID_PARAM));
    initiator = new DryRunValidator.ValidationInitiator();
    when(instanceWrapper.getInitiator()).thenReturn(initiator);
    assertEquals("VALIDATION", paramExtension.getFromInstance(Constants.INITIATOR_TYPE_PARAM));
  }

  @Test
  public void testGetFromCurrentStep() {
    String stepId = "step-123";
    String stepInstanceUuid = UUID.randomUUID().toString();
    long stepInstanceId = 2;
    long stepAttemptId = 3;
    StepRuntimeSummary summary =
        StepRuntimeSummary.builder()
            .stepId(stepId)
            .stepInstanceId(stepInstanceId)
            .stepAttemptId(stepAttemptId)
            .stepInstanceUuid(stepInstanceUuid)
            .type(StepType.NOTEBOOK)
            .subType(null)
            .stepRetry(StepInstance.StepRetry.from(null))
            .build();
    when(instanceWrapper.isWorkflowParam()).thenReturn(false);
    when(instanceWrapper.getStepInstanceAttributes())
        .thenReturn(StepInstanceAttributes.from(summary));
    assertEquals(stepId, paramExtension.getFromStep(Constants.STEP_ID_PARAM));
    assertEquals(stepInstanceId, paramExtension.getFromStep(Constants.STEP_INSTANCE_ID_PARAM));
    assertEquals(stepInstanceUuid, paramExtension.getFromStep(Constants.STEP_INSTANCE_UUID_PARAM));
    assertEquals(stepAttemptId, paramExtension.getFromStep(Constants.STEP_ATTEMPT_ID_PARAM));
    assertEquals(
        StepType.NOTEBOOK.toString(), paramExtension.getFromStep(Constants.STEP_TYPE_INFO_PARAM));
    assertEquals(0L, paramExtension.getFromStep(Constants.STEP_ERROR_RETRIES_PARAM));
  }

  @Test
  public void testGetFromStepThrowsErrorWhenInvokedFromWorkflowExecContext() {
    when(instanceWrapper.isWorkflowParam()).thenReturn(true);
    AssertHelper.assertThrows(
        "Invalid field",
        MaestroInternalError.class,
        "getFromStep throws an exception fieldName=[step_id]",
        () -> paramExtension.getFromStep(Constants.STEP_ID_PARAM));
  }

  @Test
  public void testGetExecutionEnvironment() {
    assertEquals("prod", paramExtension.callWithoutArg("getExecutionEnvironment"));
  }

  @Test
  public void testInvalidGetFromInstance() {
    when(instanceWrapper.isWorkflowParam()).thenReturn(false);
    AssertHelper.assertThrows(
        "Invalid field",
        MaestroInternalError.class,
        "getFromInstance throws an exception for fieldName=[invalid_field]",
        () -> paramExtension.getFromInstance("invalid_field"));
    AssertHelper.assertThrows(
        "Invalid call for non-workflow param",
        MaestroInternalError.class,
        "getFromInstance throws an exception for fieldName=[owner]",
        () -> paramExtension.getFromInstance(Constants.WORKFLOW_OWNER_PARAM));
    AssertHelper.assertThrows(
        "Invalid call for non-workflow param",
        MaestroInternalError.class,
        "getFromInstance throws an exception for fieldName=[FIRST_TIME_TRIGGER_TIMEZONE]",
        () -> paramExtension.getFromInstance(Constants.FIRST_TIME_TRIGGER_TIMEZONE_PARAM));

    when(instanceWrapper.getInitiator()).thenReturn(new SubworkflowInitiator());
    AssertHelper.assertThrows(
        "Invalid call for non-workflow param",
        MaestroInternalError.class,
        "getFromInstance throws an exception for fieldName=[INITIATOR_RUNNER_NAME]",
        () -> paramExtension.getFromInstance(Constants.INITIATOR_RUNNER_NAME));
  }

  @Test
  public void testGetFromSignalDependency() throws Exception {
    StepRuntimeSummary runtimeSummary =
        loadObject(
            "fixtures/execution/step-runtime-summary-with-step-dependencies.json",
            StepRuntimeSummary.class);
    when(instanceWrapper.getStepInstanceAttributes())
        .thenReturn(StepInstanceAttributes.from(runtimeSummary));
    SignalInstance instance = new SignalInstance();
    instance.setParams(Collections.singletonMap("param1", SignalParamValue.of("hello")));
    when(handler.getSignalInstance("db/test/table1", 849086)).thenReturn(instance);

    assertEquals("hello", paramExtension.getFromSignalDependency("0", "param1"));
  }

  @Test
  public void testInvalidGetFromSignalDependency() throws Exception {
    StepRuntimeSummary runtimeSummary =
        loadObject(
            "fixtures/execution/step-runtime-summary-with-step-dependencies.json",
            StepRuntimeSummary.class);
    when(instanceWrapper.getStepInstanceAttributes())
        .thenReturn(StepInstanceAttributes.from(runtimeSummary));
    SignalInstance instance = new SignalInstance();
    instance.setParams(Collections.singletonMap("param1", SignalParamValue.of("hello")));
    when(handler.getSignalInstance("db/test/table1", 849086)).thenReturn(instance);

    AssertHelper.assertThrows(
        "Referenced param in signal dependencies does not exist yet.",
        MaestroInternalError.class,
        "getFromSignalDependency throws an exception for ",
        () -> paramExtension.getFromSignalDependency("0", "param2"));

    instance.setParams(null);
    AssertHelper.assertThrows(
        "Referenced signal dependencies does not exist",
        MaestroInternalError.class,
        "getFromSignalDependency throws an exception for ",
        () -> paramExtension.getFromSignalDependency("0", "param1"));

    instance.setParams(Collections.emptyMap());
    AssertHelper.assertThrows(
        "Referenced signal dependencies does not exist",
        MaestroInternalError.class,
        "getFromSignalDependency throws an exception for ",
        () -> paramExtension.getFromSignalDependency("0", "param1"));

    instance.setParams(
        Map.of("param1", SignalParamValue.of("hello"), "param2", SignalParamValue.of("world")));
    AssertHelper.assertThrows(
        "Referenced signal dependencies does not exist",
        MaestroInternalError.class,
        "getFromSignalDependency throws an exception for ",
        () -> paramExtension.getFromSignalDependency("3", "param1"));
  }
}
