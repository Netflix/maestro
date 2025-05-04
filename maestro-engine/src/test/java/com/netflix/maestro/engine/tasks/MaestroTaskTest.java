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
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.db.DbOperation;
import com.netflix.maestro.engine.execution.StepRuntimeCallbackDelayPolicy;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.ParsableLong;
import com.netflix.maestro.models.definition.RetryPolicy;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.signal.SignalOutputs;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.jobevents.StepInstanceUpdateJobEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaestroTaskTest extends MaestroEngineBaseTest {

  private MaestroTask maestroTask;

  @Before
  public void setup() {
    maestroTask = mock(MaestroTask.class);
    doCallRealMethod().when(maestroTask).updateRetryDelayTimeToTimeline(any());
    when(maestroTask.isStepSkipped(any(), any())).thenCallRealMethod();
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
        createAndRunMaestroTask(false, stepDef, null, workflowSummary);
    return runtimeSummary.getStepRetry();
  }

  private StepRuntimeSummary createAndRunMaestroTask(
      boolean started, Step stepDef, StepRuntimeSummary input, WorkflowSummary workflowSummary) {
    maestroTask =
        new MaestroTask(
            null,
            null,
            paramEvaluator,
            MAPPER,
            null,
            mock(OutputDataManager.class),
            null,
            mock(MaestroStepInstanceActionDao.class),
            null,
            null,
            mock(StepRuntimeCallbackDelayPolicy.class),
            metricRepo,
            null,
            paramExtensionRepo);
    Task task = mock(Task.class);
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

    if (started) {
      Assert.assertTrue(maestroTask.execute(flow, task));
    } else {
      maestroTask.start(flow, task);
    }
    return (StepRuntimeSummary) runtimeSummaryMap.get(Constants.STEP_RUNTIME_SUMMARY_FIELD);
  }

  @Test
  public void testNoDynamicOutputInStepOutputs() throws Exception {
    Step stepDef = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);
    StepRuntimeSummary input =
        loadObject("fixtures/execution/sample-step-runtime-summary.json", StepRuntimeSummary.class);
    StepRuntimeSummary runtimeSummary =
        createAndRunMaestroTask(true, stepDef, input, new WorkflowSummary());

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
        createAndRunMaestroTask(true, stepDef, input, new WorkflowSummary());

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
        createAndRunMaestroTask(true, stepDef, input, new WorkflowSummary());

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
}
