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
package com.netflix.maestro.engine.tasks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.db.DbOperation;
import com.netflix.maestro.engine.db.StepAction;
import com.netflix.maestro.engine.execution.StepRuntimeCallbackDelayPolicy;
import com.netflix.maestro.engine.execution.StepRuntimeManager;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.SignalHandler;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.properties.StepTimeoutProperties;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.ParsableLong;
import com.netflix.maestro.models.definition.RetryPolicy;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.TimeoutPhase;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.StepSelection;
import com.netflix.maestro.models.instance.StepSelector;
import com.netflix.maestro.models.signal.SignalOutputs;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.jobevents.StepInstanceUpdateJobEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class MaestroTaskTest extends MaestroEngineBaseTest {
  private static final long TEN_MINUTES_IN_MILLIS = 600000L;
  private static final long ONE_MINUTE_IN_MILLIS = 60000L;
  private static final long THIRTY_MINUTES_IN_SECS = 1800L;
  private static final long FIVE_SECS = 5L;

  @Mock private MaestroStepInstanceActionDao actionDao;
  @Mock private StepRuntimeManager stepRuntimeManager;
  @Mock private MaestroTask maestroTask;
  @Mock private StepRuntimeCallbackDelayPolicy callbackDelayPolicy;
  private StepTimeoutProperties timeoutProperties;
  private Task task;

  @Before
  public void setup() {
    doCallRealMethod().when(maestroTask).updateRetryDelayTimeToTimeline(any());
    when(maestroTask.isStepSkipped(any(), any())).thenCallRealMethod();
    timeoutProperties = new StepTimeoutProperties();
  }

  @Test
  public void testUpdateRetryDelayTimeToTimeline() {
    StepRuntimeState runtimeState = new StepRuntimeState();
    runtimeState.setStatus(StepInstance.Status.USER_FAILED);
    Timeline timeline = new Timeline(new ArrayList<>());
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    RetryPolicy.FixedBackoff fixedBackoff =
        RetryPolicy.FixedBackoff.builder()
            .errorRetryBackoffInSecs(ParsableLong.of(100L))
            .platformRetryBackoffInSecs(ParsableLong.of(200L))
            .build();
    stepRetry.setBackoff(fixedBackoff);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .timeline(timeline)
            .runtimeState(runtimeState)
            .stepRetry(stepRetry)
            .build();

    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    List<TimelineEvent> timelineEvents = timeline.getTimelineEvents();
    assertThat(timelineEvents)
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Retrying task in [1m 40s]"));

    RetryPolicy.ExponentialBackoff exponentialBackoff =
        RetryPolicy.ExponentialBackoff.builder()
            .errorRetryExponent(ParsableLong.of(2))
            .errorRetryLimitInSecs(ParsableLong.of(600L))
            .errorRetryBackoffInSecs(ParsableLong.of(100L))
            .platformRetryBackoffInSecs(ParsableLong.of(200L))
            .build();
    stepRetry.setBackoff(exponentialBackoff);
    stepRetry.setErrorRetries(6);
    timelineEvents.clear();
    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    assertThat(timelineEvents)
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Retrying task in [10m]"));

    timelineEvents.clear();
    runtimeState.setStatus(StepInstance.Status.PAUSED);
    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    assertThat(timelineEvents).isEmpty();
  }

  @Test
  public void testUpdateTimeoutRetryDelayTimeToTimeline() {
    StepRuntimeState runtimeState = new StepRuntimeState();
    runtimeState.setStatus(StepInstance.Status.TIMEOUT_FAILED);
    Timeline timeline = new Timeline(new ArrayList<>());
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    RetryPolicy.FixedBackoff fixedBackoff =
        RetryPolicy.FixedBackoff.builder().timeoutRetryBackoffInSecs(ParsableLong.of(200L)).build();
    stepRetry.setBackoff(fixedBackoff);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .timeline(timeline)
            .runtimeState(runtimeState)
            .stepRetry(stepRetry)
            .build();

    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    List<TimelineEvent> timelineEvents = timeline.getTimelineEvents();
    assertThat(timelineEvents)
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Retrying task in [3m 20s]"));

    RetryPolicy.ExponentialBackoff exponentialBackoff =
        RetryPolicy.ExponentialBackoff.builder()
            .timeoutRetryExponent(ParsableLong.of(2))
            .timeoutRetryLimitInSecs(ParsableLong.of(600L))
            .timeoutRetryBackoffInSecs(ParsableLong.of(100L))
            .build();
    stepRetry.setBackoff(exponentialBackoff);
    stepRetry.setTimeoutRetries(6);
    timelineEvents.clear();
    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    assertThat(timelineEvents)
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Retrying task in [10m]"));

    timelineEvents.clear();
    runtimeState.setStatus(StepInstance.Status.PAUSED);
    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    assertThat(timelineEvents).isEmpty();
  }

  @Test
  public void testIsStepSkipped() {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId("test-workflow");
    summary.setWorkflowInstanceId(1L);
    summary.setWorkflowRunId(1L);

    StepRuntimeState runtimeState = new StepRuntimeState();
    runtimeState.setStatus(StepInstance.Status.USER_FAILED);
    Timeline timeline = new Timeline(new ArrayList<>());
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId("step1")
            .timeline(timeline)
            .runtimeState(runtimeState)
            .stepRetry(stepRetry)
            .build();
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(RestartConfig.builder().build());
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .addRestartNode("test-workflow", 2, "step1")
            .build());
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .build());
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .skipSteps(Collections.singleton("step2"))
            .stepRestartParams(Collections.singletonMap("step2", Collections.emptyMap()))
            .build());
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .skipSteps(Collections.singleton("step1"))
            .stepRestartParams(Collections.singletonMap("step1", Collections.emptyMap()))
            .build());
    Assert.assertTrue(maestroTask.isStepSkipped(summary, runtimeSummary));
    assertThat(timeline.getTimelineEvents())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Step is skipped because of a user skip action."));
    Assert.assertTrue(stepRetry.isRetryable());
    Assert.assertEquals(DbOperation.UPSERT, runtimeSummary.getDbOperation());
    Assert.assertEquals(StepInstance.Status.SKIPPED, runtimeState.getStatus());
    assertThat(runtimeSummary.getPendingRecords())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("eventTime")
        .contains(
            StepInstanceUpdateJobEvent.createRecord(
                StepInstance.Status.USER_FAILED, StepInstance.Status.SKIPPED, 0L));

    summary.setRestartConfig(null);
    runtimeSummary.getPendingRecords().clear();
    runtimeState.setStatus(StepInstance.Status.USER_FAILED);
    runtimeSummary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .skipSteps(Collections.singleton("step1"))
            .stepRestartParams(Collections.singletonMap("step1", Collections.emptyMap()))
            .build());
    Assert.assertTrue(maestroTask.isStepSkipped(summary, runtimeSummary));
    assertThat(timeline.getTimelineEvents())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Step is skipped because of a user skip action."));
    Assert.assertTrue(stepRetry.isRetryable());
    Assert.assertEquals(DbOperation.UPSERT, runtimeSummary.getDbOperation());
    Assert.assertEquals(StepInstance.Status.SKIPPED, runtimeState.getStatus());
    assertThat(runtimeSummary.getPendingRecords())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("eventTime")
        .contains(
            StepInstanceUpdateJobEvent.createRecord(
                StepInstance.Status.USER_FAILED, StepInstance.Status.SKIPPED, 0L));
  }

  @Test
  public void testIsStepSkippedByStepSelection() {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId("test-workflow");
    summary.setWorkflowInstanceId(1L);
    summary.setWorkflowRunId(1L);

    StepRuntimeState runtimeState = new StepRuntimeState();
    runtimeState.setStatus(StepInstance.Status.NOT_CREATED);
    Timeline timeline = new Timeline(new ArrayList<>());
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId("load_expensive")
            .timeline(timeline)
            .runtimeState(runtimeState)
            .stepRetry(stepRetry)
            .build();

    // no selection at all leaves the step alone
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    // included and not excluded, so it runs
    summary.setStepSelection(
        StepSelection.builder()
            .include(StepSelector.builder().stepIdStartsWith(Set.of("load_")).build())
            .build());
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    // exclude overrides include
    summary.setStepSelection(
        StepSelection.builder()
            .include(StepSelector.builder().stepIdStartsWith(Set.of("load_")).build())
            .exclude(StepSelector.builder().stepIds(Set.of("load_expensive")).build())
            .build());
    Assert.assertTrue(maestroTask.isStepSkipped(summary, runtimeSummary));
    Assert.assertEquals(StepInstance.Status.SKIPPED, runtimeState.getStatus());
    assertThat(timeline.getTimelineEvents())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(
            TimelineLogEvent.info(
                "Step is skipped because it matches the excluded ids [load_expensive]."));
  }

  @Test
  public void testIsStepSkippedWhenNotIncluded() {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId("test-workflow");
    summary.setWorkflowInstanceId(1L);
    summary.setWorkflowRunId(1L);
    summary.setStepSelection(
        StepSelection.builder()
            .include(StepSelector.builder().stepIdStartsWith(Set.of("load_")).build())
            .build());

    StepRuntimeState runtimeState = new StepRuntimeState();
    runtimeState.setStatus(StepInstance.Status.NOT_CREATED);
    Timeline timeline = new Timeline(new ArrayList<>());
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId("transform")
            .timeline(timeline)
            .runtimeState(runtimeState)
            .stepRetry(stepRetry)
            .build();

    Assert.assertTrue(maestroTask.isStepSkipped(summary, runtimeSummary));
    Assert.assertEquals(StepInstance.Status.SKIPPED, runtimeState.getStatus());
    assertThat(timeline.getTimelineEvents())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(
            TimelineLogEvent.info(
                "Step is skipped because it does not match the included starts_with [load_]."));
  }

  @Test
  public void testParseRetryPolicy() throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    StepInstance.StepRetry actual = initializeStepRetry(false, workflowSummary);

    // Verify retry parameters are correctly parsed.
    Assert.assertEquals(5, actual.getErrorRetryLimit());
    Assert.assertEquals(3, actual.getPlatformRetryLimit());
    Assert.assertEquals(1, actual.getTimeoutRetryLimit());
    Assert.assertEquals(200, actual.getBackoff().getNextRetryDelayForUserError(1));
    Assert.assertEquals(350, actual.getBackoff().getNextRetryDelayForUserError(2));
    Assert.assertEquals(900, actual.getBackoff().getNextRetryDelayForPlatformError(1));
    Assert.assertEquals(1000, actual.getBackoff().getNextRetryDelayForPlatformError(2));
    Assert.assertEquals(1000, actual.getBackoff().getNextRetryDelayForTimeoutError(1));
    Assert.assertEquals(5000, actual.getBackoff().getNextRetryDelayForTimeoutError(2));
  }

  @Test
  public void testParseRetryPolicyWithParams() throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setParams(
        Map.of(
            "foo", buildParam("foo", 3L),
            "far", buildParam("far", 100L),
            "bar", buildParam("bar", 10L),
            "bat", buildParam("bat", "2"),
            "baz", buildParam("baz", "30min")));
    StepInstance.StepRetry actual = initializeStepRetry(true, workflowSummary);

    // Verify retry parameters are correctly parsed.
    Assert.assertEquals(3, actual.getErrorRetryLimit());
    Assert.assertEquals(10, actual.getPlatformRetryLimit());
    Assert.assertEquals(2, actual.getTimeoutRetryLimit());
    Assert.assertEquals(200, actual.getBackoff().getNextRetryDelayForUserError(1));
    Assert.assertEquals(800, actual.getBackoff().getNextRetryDelayForUserError(3));
    Assert.assertEquals(1800, actual.getBackoff().getNextRetryDelayForUserError(10));
    Assert.assertEquals(200, actual.getBackoff().getNextRetryDelayForPlatformError(1));
    Assert.assertEquals(800, actual.getBackoff().getNextRetryDelayForPlatformError(3));
    Assert.assertEquals(1800, actual.getBackoff().getNextRetryDelayForPlatformError(10));
    Assert.assertEquals(200, actual.getBackoff().getNextRetryDelayForTimeoutError(1));
    Assert.assertEquals(800, actual.getBackoff().getNextRetryDelayForTimeoutError(3));
    Assert.assertEquals(1800, actual.getBackoff().getNextRetryDelayForTimeoutError(10));
  }

  @Test
  public void testParseRetryPolicyWithErrorFallback() throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setParams(
        Map.of(
            "foo", buildParam("foo", 3L),
            "far", buildParam("far", 100L),
            "bar", buildParam("bar", 100L),
            "bat", buildParam("bat", "2"),
            "baz", buildParam("baz", "1800")));
    StepInstance.StepRetry actual = initializeStepRetry(true, workflowSummary);

    // Verify retry parameters use fallback default.
    Assert.assertEquals(2, actual.getErrorRetryLimit());
    Assert.assertEquals(10, actual.getPlatformRetryLimit());
    Assert.assertEquals(0, actual.getTimeoutRetryLimit());
    Assert.assertEquals(Defaults.DEFAULT_EXPONENTIAL_BACK_OFF, actual.getBackoff());
  }

  @Test
  public void testParseRetryPolicyWithInvalidDurationFallback() throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setParams(
        Map.of(
            "foo", buildParam("foo", 3L),
            "far", buildParam("far", 100L),
            "bar", buildParam("bar", 10L),
            "bat", buildParam("bat", "2"),
            "baz", buildParam("baz", "abc")));
    StepInstance.StepRetry actual = initializeStepRetry(true, workflowSummary);

    // Verify retry parameters use fallback default.
    Assert.assertEquals(2, actual.getErrorRetryLimit());
    Assert.assertEquals(10, actual.getPlatformRetryLimit());
    Assert.assertEquals(0, actual.getTimeoutRetryLimit());
    Assert.assertEquals(Defaults.DEFAULT_EXPONENTIAL_BACK_OFF, actual.getBackoff());
  }

  @Test
  public void testParseRetryPolicyWithNotFoundParamFallback() throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setParams(Map.of());
    StepInstance.StepRetry actual = initializeStepRetry(true, workflowSummary);

    // Verify retry parameters use fallback default.
    Assert.assertEquals(2, actual.getErrorRetryLimit());
    Assert.assertEquals(10, actual.getPlatformRetryLimit());
    Assert.assertEquals(0, actual.getTimeoutRetryLimit());
    Assert.assertEquals(Defaults.DEFAULT_EXPONENTIAL_BACK_OFF, actual.getBackoff());
  }

  private StepInstance.StepRetry initializeStepRetry(
      boolean withParams, WorkflowSummary workflowSummary) throws Exception {
    Step stepDef =
        withParams
            ? loadObject("fixtures/typedsteps/sample-step-with-param-retries.json", Step.class)
            : loadObject("fixtures/typedsteps/sample-step-with-retries.json", Step.class);
    StepRuntimeSummary runtimeSummary =
        createAndRunMaestroTask(0, null, stepDef, null, workflowSummary);
    return runtimeSummary.getStepRetry();
  }

  private StepRuntimeSummary createAndRunMaestroTask(
      int runCode,
      Flow.Status status,
      Step stepDef,
      StepRuntimeSummary input,
      WorkflowSummary workflowSummary) {
    SignalHandler signalHandler = mock(SignalHandler.class);
    when(signalHandler.sendOutputSignals(any(), any())).thenReturn(true);
    maestroTask =
        new MaestroTask(
            stepRuntimeManager,
            null,
            paramEvaluator,
            MAPPER,
            signalHandler,
            mock(OutputDataManager.class),
            null,
            actionDao,
            null,
            null,
            callbackDelayPolicy,
            timeoutProperties,
            metricRepo,
            null,
            paramExtensionRepo);
    task = mock(Task.class);
    when(task.referenceTaskName()).thenReturn("job1");
    Map<String, Object> runtimeSummaryMap = new HashMap<>();
    runtimeSummaryMap.put(Constants.STEP_RUNTIME_SUMMARY_FIELD, input);
    when(task.getOutputData()).thenReturn(runtimeSummaryMap);

    Flow flow = mock(Flow.class);
    workflowSummary.setWorkflowId("test-workflow");
    workflowSummary.setWorkflowInstanceId(1L);
    workflowSummary.setWorkflowRunId(1L);
    workflowSummary.setStepMap(Map.of("job1", stepDef));
    when(flow.getInput()).thenReturn(Map.of(Constants.WORKFLOW_SUMMARY_FIELD, workflowSummary));
    when(flow.getPrepareTask()).thenReturn(task);
    when(flow.getStatus()).thenReturn(status);

    if (runCode == 0) { // start
      maestroTask.start(flow, task);
    } else if (runCode == 1) { // execute
      Assert.assertTrue(maestroTask.execute(flow, task));
    } else if (runCode == 2) { // cancel
      maestroTask.cancel(flow, task);
    } else { // execute without asserting on the returned sync flag
      maestroTask.execute(flow, task);
    }
    return (StepRuntimeSummary) runtimeSummaryMap.get(Constants.STEP_RUNTIME_SUMMARY_FIELD);
  }

  @Test
  public void testNoDynamicOutputInStepOutputs() throws Exception {
    Step stepDef = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);
    StepRuntimeSummary input =
        loadObject("fixtures/execution/sample-step-runtime-summary.json", StepRuntimeSummary.class);
    StepRuntimeSummary runtimeSummary =
        createAndRunMaestroTask(1, null, stepDef, input, new WorkflowSummary());

    // verify there is only one static signal
    SignalOutputs outputs = runtimeSummary.getSignalOutputs();
    Assert.assertEquals(1, outputs.getOutputs().size());
  }

  @Test
  public void testOnlyDynamicOutputInStepOutputs() throws Exception {
    Step stepDef = loadObject("fixtures/typedsteps/sample-step-with-retries.json", Step.class);
    StepRuntimeSummary input =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-with-dynamic-output.json",
            StepRuntimeSummary.class);
    StepRuntimeSummary runtimeSummary =
        createAndRunMaestroTask(1, null, stepDef, input, new WorkflowSummary());

    // verify there are two dynamic signals
    SignalOutputs outputs = runtimeSummary.getSignalOutputs();
    Assert.assertEquals(2, outputs.getOutputs().size());
    Set<String> dynamicOutputNames = new HashSet<>();
    for (SignalOutputs.SignalOutput output : outputs.getOutputs()) {
      String tableName = output.getName();
      dynamicOutputNames.add(tableName);
    }
    Assert.assertEquals(Set.of("table_1", "table_2"), dynamicOutputNames);
  }

  @Test
  public void testOutputInStepOutputs() throws Exception {
    Step stepDef = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);
    StepRuntimeSummary input =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-with-dynamic-output.json",
            StepRuntimeSummary.class);
    StepRuntimeSummary runtimeSummary =
        createAndRunMaestroTask(1, null, stepDef, input, new WorkflowSummary());

    // verify there are 3 output signals:  1 static + 2 dynamic
    SignalOutputs outputs = runtimeSummary.getSignalOutputs();
    Assert.assertEquals(3, outputs.getOutputs().size());
    boolean[] signalFound = new boolean[] {false, false, false}; // static, dynamic_1, dynamic_2
    for (SignalOutputs.SignalOutput output : outputs.getOutputs()) {
      String tableName = output.getName();
      if (tableName.startsWith("table_")) {
        int index = Integer.parseInt(tableName.substring(6));
        signalFound[index] = true;
      }
      if (tableName.equals("table_1")) {
        Map<String, Object> evaluated = output.getPayload();
        Assert.assertTrue((Boolean) evaluated.get("is_iceberg"));
        Map<String, Object> nestedMap = (Map<String, Object>) evaluated.get("nested_map");
        Assert.assertArrayEquals(
            new String[] {"a", "b", "c"}, (String[]) nestedMap.get("nested_string_array"));
        Assert.assertArrayEquals(new long[] {1, 2, 3}, (long[]) nestedMap.get("nested_long_array"));
        Assert.assertArrayEquals(
            new boolean[] {true, false, true}, (boolean[]) nestedMap.get("nested_boolean_array"));
        Assert.assertArrayEquals(
            new double[] {1.1, 2.2, 3.3}, (double[]) nestedMap.get("nested_double_array"), 0.001);
        Map<String, String> nestedStringMap =
            (Map<String, String>) nestedMap.get("nested_string_map");
        Assert.assertEquals(1, nestedStringMap.size());
        Assert.assertEquals("bar", nestedStringMap.get("foo"));
      }
    }
    Assert.assertArrayEquals(new boolean[] {true, true, true}, signalFound);
  }

  @Test
  public void testCancelWithTimeOutFlowStatus() throws Exception {
    testCancel(Flow.Status.TIMED_OUT, StepInstance.Status.TIMED_OUT);
  }

  @Test
  public void testCancelWithStoppedFlowStatus() throws Exception {
    testCancel(Flow.Status.RUNNING, StepInstance.Status.STOPPED);
  }

  private void testCancel(Flow.Status flowStatus, StepInstance.Status stepStatus) throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    Step stepDefinition = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);
    StepRuntimeSummary runtimeSummary =
        loadObject("fixtures/execution/sample-step-runtime-summary.json", StepRuntimeSummary.class);

    createAndRunMaestroTask(2, flowStatus, stepDefinition, runtimeSummary, workflowSummary);

    verify(stepRuntimeManager).terminate(eq(workflowSummary), eq(runtimeSummary), eq(stepStatus));
  }

  @Test
  public void testInitializeTimeoutWithPhases() throws Exception {
    Step stepDef =
        loadObject("fixtures/typedsteps/sample-typed-step-with-timeouts.json", Step.class);
    WorkflowSummary workflowSummary = new WorkflowSummary();
    StepRuntimeSummary runtimeSummary =
        createAndRunMaestroTask(0, null, stepDef, null, workflowSummary);

    maestroTask.initializeTimeout(stepDef, workflowSummary, runtimeSummary);

    Assert.assertNull(runtimeSummary.getTimeoutInMillis());
    Map<TimeoutPhase, Long> expected = new EnumMap<>(TimeoutPhase.class);
    expected.put(TimeoutPhase.STEP, 86400000L);
    expected.put(TimeoutPhase.WAITING_FOR_SIGNALS, 14400000L);
    expected.put(TimeoutPhase.WAITING_FOR_PERMITS, 7200000L);
    expected.put(TimeoutPhase.RUNNING, 28800000L);
    Assert.assertEquals(expected, runtimeSummary.getTimeoutsInMillis());
  }

  @Test
  public void testInitializeTimeoutFillsDefaultPhaseFromSingleTimeout() throws Exception {
    Step stepDef = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);
    WorkflowSummary workflowSummary = new WorkflowSummary();
    StepRuntimeSummary runtimeSummary =
        createAndRunMaestroTask(0, null, stepDef, null, workflowSummary);

    maestroTask.initializeTimeout(stepDef, workflowSummary, runtimeSummary);
    Assert.assertEquals(TEN_MINUTES_IN_MILLIS, runtimeSummary.getTimeoutInMillis().longValue());
    Assert.assertEquals(
        Map.of(TimeoutPhase.RUNNING, TEN_MINUTES_IN_MILLIS), runtimeSummary.getTimeoutsInMillis());

    timeoutProperties.setDefaultTimeoutPhase(TimeoutPhase.STEP);
    runtimeSummary = createAndRunMaestroTask(0, null, stepDef, null, workflowSummary);
    maestroTask.initializeTimeout(stepDef, workflowSummary, runtimeSummary);
    Assert.assertEquals(TEN_MINUTES_IN_MILLIS, runtimeSummary.getTimeoutInMillis().longValue());
    Assert.assertEquals(
        Map.of(TimeoutPhase.STEP, TEN_MINUTES_IN_MILLIS), runtimeSummary.getTimeoutsInMillis());
  }

  @Test
  public void testInitializeTimeoutWithoutAnyTimeout() throws Exception {
    Step stepDef = loadObject("fixtures/typedsteps/sample-while-step.json", Step.class);
    WorkflowSummary workflowSummary = new WorkflowSummary();
    StepRuntimeSummary runtimeSummary =
        createAndRunMaestroTask(0, null, stepDef, null, workflowSummary);

    maestroTask.initializeTimeout(stepDef, workflowSummary, runtimeSummary);
    Assert.assertNull(runtimeSummary.getTimeoutInMillis());
    Assert.assertNull(runtimeSummary.getTimeoutsInMillis());
  }

  @Test
  public void testExecuteTimedOutInRunningPhase() throws Exception {
    StepRuntimeSummary runtimeSummary = loadTimeoutRuntimeSummary(StepInstance.Status.FINISHING);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setStartTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.setTimeoutsInMillis(Map.of(TimeoutPhase.RUNNING, TEN_MINUTES_IN_MILLIS));

    assertTimedOut(runtimeSummary, TimeoutPhase.RUNNING);
  }

  @Test
  public void testExecuteTimedOutInStepPhase() throws Exception {
    StepRuntimeSummary runtimeSummary = loadTimeoutRuntimeSummary(StepInstance.Status.FINISHING);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setStartTime(now - 1000);
    runtimeSummary.setTimeoutsInMillis(Map.of(TimeoutPhase.STEP, TEN_MINUTES_IN_MILLIS));

    assertTimedOut(runtimeSummary, TimeoutPhase.STEP);
  }

  @Test
  public void testExecuteTimedOutWaitingForSignals() throws Exception {
    StepRuntimeSummary runtimeSummary =
        loadTimeoutRuntimeSummary(StepInstance.Status.WAITING_FOR_SIGNALS);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setWaitSignalTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.setTimeoutsInMillis(
        Map.of(TimeoutPhase.WAITING_FOR_SIGNALS, TEN_MINUTES_IN_MILLIS));

    assertTimedOut(runtimeSummary, TimeoutPhase.WAITING_FOR_SIGNALS);
  }

  @Test
  public void testExecuteTimedOutWaitingForPermits() throws Exception {
    StepRuntimeSummary runtimeSummary =
        loadTimeoutRuntimeSummary(StepInstance.Status.WAITING_FOR_PERMITS);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setWaitPermitTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.setTimeoutsInMillis(
        Map.of(TimeoutPhase.WAITING_FOR_PERMITS, TEN_MINUTES_IN_MILLIS));

    assertTimedOut(runtimeSummary, TimeoutPhase.WAITING_FOR_PERMITS);
  }

  @Test
  public void testExecuteWaitingPhaseTimeoutIgnoredInOtherStatus() throws Exception {
    StepRuntimeSummary runtimeSummary = loadTimeoutRuntimeSummary(StepInstance.Status.FINISHING);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setWaitSignalTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setWaitPermitTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setStartTime(now - 1000);
    runtimeSummary.setTimeoutsInMillis(
        Map.of(
            TimeoutPhase.WAITING_FOR_SIGNALS, TEN_MINUTES_IN_MILLIS,
            TimeoutPhase.WAITING_FOR_PERMITS, TEN_MINUTES_IN_MILLIS));

    assertNotTimedOut(runtimeSummary);
  }

  @Test
  public void testExecuteTimedOutWithSingleTimeoutOnly() throws Exception {
    StepRuntimeSummary runtimeSummary = loadTimeoutRuntimeSummary(StepInstance.Status.FINISHING);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setStartTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.setTimeoutInMillis(TEN_MINUTES_IN_MILLIS);

    assertTimedOut(runtimeSummary, TimeoutPhase.RUNNING);
  }

  @Test
  public void testExecuteNotTimedOutWithinDefaultRunningTimeout() throws Exception {
    StepRuntimeSummary runtimeSummary = loadTimeoutRuntimeSummary(StepInstance.Status.FINISHING);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setStartTime(now - TEN_MINUTES_IN_MILLIS - 1000);

    assertNotTimedOut(runtimeSummary);
  }

  @Test
  public void testExecuteTimedOutByDefaultStepTimeout() throws Exception {
    timeoutProperties.setDefaultStepTimeoutInMillis(TEN_MINUTES_IN_MILLIS);
    StepRuntimeSummary runtimeSummary = loadTimeoutRuntimeSummary(StepInstance.Status.FINISHING);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setStartTime(now - 1000);

    assertTimedOut(runtimeSummary, TimeoutPhase.STEP);
  }

  @Test
  public void testExecuteTimedOutByDefaultWaitingForSignalsTimeout() throws Exception {
    timeoutProperties.setDefaultWaitingForSignalsTimeoutInMillis(TEN_MINUTES_IN_MILLIS);
    StepRuntimeSummary runtimeSummary =
        loadTimeoutRuntimeSummary(StepInstance.Status.WAITING_FOR_SIGNALS);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setWaitSignalTime(now - TEN_MINUTES_IN_MILLIS - 1000);

    assertTimedOut(runtimeSummary, TimeoutPhase.WAITING_FOR_SIGNALS);
  }

  @Test
  public void testExecuteTimedOutByDefaultWaitingForPermitsTimeout() throws Exception {
    timeoutProperties.setDefaultWaitingForPermitsTimeoutInMillis(TEN_MINUTES_IN_MILLIS);
    StepRuntimeSummary runtimeSummary =
        loadTimeoutRuntimeSummary(StepInstance.Status.WAITING_FOR_PERMITS);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setWaitPermitTime(now - TEN_MINUTES_IN_MILLIS - 1000);

    assertTimedOut(runtimeSummary, TimeoutPhase.WAITING_FOR_PERMITS);
  }

  @Test
  public void testPollDelayCappedByWaitingForSignalsTimeout() throws Exception {
    when(callbackDelayPolicy.getCallBackDelayInSecs(any())).thenReturn(THIRTY_MINUTES_IN_SECS);
    StepRuntimeSummary runtimeSummary =
        loadTimeoutRuntimeSummary(StepInstance.Status.WAITING_FOR_SIGNALS);
    long now = System.currentTimeMillis();
    runtimeSummary
        .getRuntimeState()
        .setCreateTime(now - TEN_MINUTES_IN_MILLIS + ONE_MINUTE_IN_MILLIS);
    runtimeSummary
        .getRuntimeState()
        .setWaitSignalTime(now - TEN_MINUTES_IN_MILLIS + ONE_MINUTE_IN_MILLIS);
    runtimeSummary.setTimeoutsInMillis(
        Map.of(TimeoutPhase.WAITING_FOR_SIGNALS, TEN_MINUTES_IN_MILLIS));

    assertFirstStartDelayWithin(runtimeSummary, 0, ONE_MINUTE_IN_MILLIS);
  }

  @Test
  public void testPollDelayCappedByRunningTimeout() throws Exception {
    when(callbackDelayPolicy.getCallBackDelayInSecs(any())).thenReturn(THIRTY_MINUTES_IN_SECS);
    StepRuntimeSummary runtimeSummary = loadTimeoutRuntimeSummary(StepInstance.Status.FINISHING);
    long now = System.currentTimeMillis();
    runtimeSummary
        .getRuntimeState()
        .setCreateTime(now - TEN_MINUTES_IN_MILLIS + ONE_MINUTE_IN_MILLIS);
    runtimeSummary
        .getRuntimeState()
        .setStartTime(now - TEN_MINUTES_IN_MILLIS + ONE_MINUTE_IN_MILLIS);
    runtimeSummary.setTimeoutsInMillis(Map.of(TimeoutPhase.RUNNING, TEN_MINUTES_IN_MILLIS));

    assertFirstStartDelayWithin(runtimeSummary, 0, ONE_MINUTE_IN_MILLIS);
  }

  @Test
  public void testPollDelayUnchangedWhenTimeoutIsFurtherAway() throws Exception {
    when(callbackDelayPolicy.getCallBackDelayInSecs(any())).thenReturn(FIVE_SECS);
    StepRuntimeSummary runtimeSummary = loadTimeoutRuntimeSummary(StepInstance.Status.FINISHING);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - ONE_MINUTE_IN_MILLIS);
    runtimeSummary.getRuntimeState().setStartTime(now - ONE_MINUTE_IN_MILLIS);
    runtimeSummary.setTimeoutsInMillis(Map.of(TimeoutPhase.RUNNING, TEN_MINUTES_IN_MILLIS));

    assertFirstStartDelayWithin(
        runtimeSummary, TimeUnit.SECONDS.toMillis(FIVE_SECS), TimeUnit.SECONDS.toMillis(FIVE_SECS));
  }

  @Test
  public void testPollDelayIsZeroWhenAlreadyTimedOut() throws Exception {
    when(callbackDelayPolicy.getCallBackDelayInSecs(any())).thenReturn(THIRTY_MINUTES_IN_SECS);
    StepRuntimeSummary runtimeSummary = loadTimeoutRuntimeSummary(StepInstance.Status.FINISHING);
    long now = System.currentTimeMillis();
    runtimeSummary.getRuntimeState().setCreateTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.getRuntimeState().setStartTime(now - TEN_MINUTES_IN_MILLIS - 1000);
    runtimeSummary.setTimeoutsInMillis(Map.of(TimeoutPhase.RUNNING, TEN_MINUTES_IN_MILLIS));

    assertFirstStartDelayWithin(runtimeSummary, 0, 0);
  }

  private void assertFirstStartDelayWithin(
      StepRuntimeSummary runtimeSummary, long minInclusive, long maxInclusive) throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    Step stepDef = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);

    createAndRunMaestroTask(3, Flow.Status.RUNNING, stepDef, runtimeSummary, workflowSummary);

    ArgumentCaptor<Long> delay = ArgumentCaptor.forClass(Long.class);
    verify(task, atLeastOnce()).setStartDelayInMillis(delay.capture());
    long firstDelay = delay.getAllValues().get(0);
    assertThat(firstDelay).isBetween(minInclusive, maxInclusive);
  }

  private StepRuntimeSummary loadTimeoutRuntimeSummary(StepInstance.Status status)
      throws Exception {
    StepRuntimeSummary runtimeSummary =
        loadObject("fixtures/execution/sample-step-runtime-summary.json", StepRuntimeSummary.class);
    runtimeSummary.getRuntimeState().setStatus(status);
    return runtimeSummary;
  }

  private void assertTimedOut(StepRuntimeSummary runtimeSummary, TimeoutPhase phase)
      throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    Step stepDef = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);

    createAndRunMaestroTask(1, Flow.Status.RUNNING, stepDef, runtimeSummary, workflowSummary);

    verify(stepRuntimeManager)
        .terminate(eq(workflowSummary), eq(runtimeSummary), eq(StepInstance.Status.TIMED_OUT));
    assertThat(runtimeSummary.getTimeline().getTimelineEvents())
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(
            TimelineLogEvent.info(
                "Step instance is timed out in phase [%s] after [10m].", phase.name()));
  }

  private void assertNotTimedOut(StepRuntimeSummary runtimeSummary) throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    Step stepDef = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);

    createAndRunMaestroTask(1, Flow.Status.RUNNING, stepDef, runtimeSummary, workflowSummary);

    verify(stepRuntimeManager, never()).terminate(any(), any(), any());
  }

  @Test
  public void testExecuteWithTimeOutAction() throws Exception {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    Step stepDef = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);
    StepRuntimeSummary runtimeSummary =
        loadObject("fixtures/execution/sample-step-runtime-summary.json", StepRuntimeSummary.class);

    StepAction timeoutAction =
        StepAction.builder()
            .action(Actions.StepInstanceAction.TIME_OUT)
            .workflowId("test-workflow")
            .workflowInstanceId(1)
            .build();
    when(actionDao.tryGetAction(workflowSummary, stepDef.getId()))
        .thenReturn(Optional.of(timeoutAction));

    createAndRunMaestroTask(1, Flow.Status.RUNNING, stepDef, runtimeSummary, workflowSummary);

    verify(stepRuntimeManager)
        .terminate(eq(workflowSummary), eq(runtimeSummary), eq(StepInstance.Status.TIMED_OUT));
  }
}
