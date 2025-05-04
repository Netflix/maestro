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
package com.netflix.maestro.engine.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.db.StepAction;
import com.netflix.maestro.engine.tracing.MaestroTracingContext;
import com.netflix.maestro.engine.tracing.MaestroTracingManager;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.DefaultArtifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.artifact.SubworkflowArtifact;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.ForeachStepOverview;
import com.netflix.maestro.models.instance.StepDependencyMatchStatus;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.models.signal.SignalDependencies;
import com.netflix.maestro.models.signal.SignalDependenciesDefinition;
import com.netflix.maestro.models.signal.SignalMatchParamDef;
import com.netflix.maestro.models.signal.SignalOperator;
import com.netflix.maestro.models.signal.SignalOutputs;
import com.netflix.maestro.models.signal.SignalOutputsDefinition;
import com.netflix.maestro.models.signal.SignalTransformer;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

public class StepRuntimeSummaryTest extends MaestroEngineBaseTest {

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    var definition = new SignalDependenciesDefinition.SignalDependencyDefinition();
    definition.setName("signal_a");
    definition.setMatchParams(
        Map.of(
            "param_a",
            SignalMatchParamDef.builder()
                .operator(SignalOperator.EQUALS_TO)
                .param(StringParamDefinition.builder().name("param_a").value("test123").build())
                .build()));
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put("name", StringParamDefinition.builder().name("name").value("signal_a").build());
    paramDefMap.put(
        "param_a", StringParamDefinition.builder().name("param_a").value("test123").build());
    Map<String, Object> evaluatedResult = new HashMap<>();
    evaluatedResult.put("name", "signal_a");
    evaluatedResult.put("param_a", "test123");

    MapParameter mapParameter =
        MapParameter.builder()
            .value(paramDefMap)
            .evaluatedResult(evaluatedResult)
            .evaluatedTime(12345L)
            .build();

    SignalDependencies signalDependencies = new SignalDependencies();
    signalDependencies.setDependencies(
        Collections.singletonList(SignalTransformer.transform(definition, mapParameter)));

    SignalOutputs signalOutputs = new SignalOutputs();
    var outputDef = new SignalOutputsDefinition.SignalOutputDefinition();
    outputDef.setName("signal_a");
    outputDef.setParams(paramDefMap);
    signalOutputs.setOutputs(
        Collections.singletonList(SignalTransformer.transform(outputDef, mapParameter)));

    signalDependencies.bypass(User.builder().name("user").build(), System.currentTimeMillis());

    Map<String, Parameter> params = new LinkedHashMap<>();
    params.put(
        "param1",
        StringParameter.builder()
            .value("foo")
            .evaluatedResult("foo")
            .evaluatedTime(System.currentTimeMillis())
            .build());

    StepRuntimeSummary summary =
        StepRuntimeSummary.builder()
            .stepId("foo")
            .stepAttemptId(2)
            .stepInstanceUuid("bar")
            .stepName("step1")
            .stepInstanceId(123)
            .params(params)
            .signalDependencies(signalDependencies)
            .signalOutputs(signalOutputs)
            .build();
    summary.markTerminated(StepInstance.Status.SUCCEEDED, null);
    summary.addTimeline(TimelineLogEvent.info("hello"));
    String ser1 = MAPPER.writeValueAsString(summary);
    StepRuntimeSummary actual = MAPPER.readValue(ser1, StepRuntimeSummary.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(summary.getSignalDependencies(), actual.getSignalDependencies());
    assertEquals(summary.getSignalOutputs(), actual.getSignalOutputs());
    assertEquals(ser1, ser2);
  }

  @Test
  public void testMergeDefaultArtifact() throws Exception {
    StepRuntimeSummary summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-1.json", StepRuntimeSummary.class);
    DefaultArtifact artifact = summary.getArtifacts().get("artifact1").asDefault();
    assertEquals(1L, artifact.getValue());
    assertEquals("bar", artifact.getField("foo"));
    Map<String, Artifact> artifacts = new LinkedHashMap<>();
    DefaultArtifact artifact1 = new DefaultArtifact();
    artifact1.setValue(12L);
    artifact1.add("value", 123L);
    artifact1.add("bar", true);
    artifact1.add("baz", 123L);
    artifacts.put("artifact1", artifact1);
    assertTrue(summary.isSynced());
    summary.mergeRuntimeUpdate(null, artifacts);
    assertFalse(summary.isSynced());
    String ser1 = MAPPER.writeValueAsString(summary);
    StepRuntimeSummary actual = MAPPER.readValue(ser1, StepRuntimeSummary.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(summary, actual);
    assertEquals(ser1, ser2);
    artifact1 = summary.getArtifacts().get("artifact1").asDefault();
    assertEquals(123L, artifact1.getValue());
    assertEquals(123L, artifact1.getField("value"));
    assertEquals("bar", artifact1.getField("foo"));
    assertEquals(true, artifact1.getField("bar"));
    assertEquals(123L, artifact1.getField("baz"));
  }

  @Test
  public void testStepRuntimeParams() throws Exception {
    StepRuntimeSummary summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-1.json", StepRuntimeSummary.class);
    Map<String, ParamDefinition> stepRunParams = summary.getStepRunParams();
    assertEquals("bar", stepRunParams.get("foo").asStringParamDef().getValue());
  }

  @Test
  public void testMergeSubworkflowArtifact() throws Exception {
    StepRuntimeSummary summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-1.json", StepRuntimeSummary.class);
    SubworkflowArtifact artifact =
        summary.getArtifacts().get(Artifact.Type.SUBWORKFLOW.key()).asSubworkflow();
    assertEquals("test-dag", artifact.getSubworkflowId());
    assertEquals(1L, artifact.getSubworkflowInstanceId());
    assertEquals(1L, artifact.getSubworkflowRunId());
    assertEquals(
        Collections.singletonMap(StepInstance.Status.SUCCEEDED, WorkflowStepStatusSummary.of(1L)),
        artifact.getSubworkflowOverview().getStepOverview());
    assertEquals(1L, artifact.getSubworkflowOverview().getTotalStepCount());
    SubworkflowArtifact artifact1 = new SubworkflowArtifact();
    artifact1.setSubworkflowId("updated");
    assertTrue(summary.isSynced());
    summary.mergeRuntimeUpdate(
        null, Collections.singletonMap(Artifact.Type.SUBWORKFLOW.key(), artifact1));
    assertFalse(summary.isSynced());
    String ser1 = MAPPER.writeValueAsString(summary);
    StepRuntimeSummary actual = MAPPER.readValue(ser1, StepRuntimeSummary.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(summary, actual);
    assertEquals(ser1, ser2);
    assertEquals(
        "updated",
        summary
            .getArtifacts()
            .get(Artifact.Type.SUBWORKFLOW.key())
            .asSubworkflow()
            .getSubworkflowId());
  }

  @Test
  public void testMergeForeachArtifact() throws Exception {
    StepRuntimeSummary summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-3.json", StepRuntimeSummary.class);
    ForeachArtifact artifact = summary.getArtifacts().get(Artifact.Type.FOREACH.key()).asForeach();
    assertEquals("inline-wf", artifact.getForeachWorkflowId());
    assertEquals("foo", artifact.getForeachIdentity());
    assertEquals(10, artifact.getTotalLoopCount());
    assertEquals(0, artifact.getNextLoopIndex());
    assertEquals(
        5L,
        artifact.getForeachOverview().getStats().get(WorkflowInstance.Status.CREATED).longValue());
    assertEquals(
        1L,
        artifact
            .getForeachOverview()
            .getStats()
            .get(WorkflowInstance.Status.SUCCEEDED)
            .longValue());
    ForeachArtifact artifact1 = new ForeachArtifact();
    artifact1.setForeachWorkflowId("updated");
    assertTrue(summary.isSynced());
    summary.mergeRuntimeUpdate(
        null, Collections.singletonMap(Artifact.Type.FOREACH.key(), artifact1));
    assertFalse(summary.isSynced());
    String ser1 = MAPPER.writeValueAsString(summary);
    StepRuntimeSummary actual = MAPPER.readValue(ser1, StepRuntimeSummary.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(summary, actual);
    assertEquals(ser1, ser2);
    assertEquals(
        "updated",
        summary.getArtifacts().get(Artifact.Type.FOREACH.key()).asForeach().getForeachWorkflowId());
    assertNull(
        summary.getArtifacts().get(Artifact.Type.FOREACH.key()).asForeach().getForeachOverview());
  }

  @Test
  public void testInitializeStepSignalSummary() {
    Map<String, Parameter> params = new LinkedHashMap<>();
    params.put(
        "param1",
        StringParameter.builder()
            .value("foo")
            .evaluatedResult("foo")
            .evaluatedTime(System.currentTimeMillis())
            .build());
    StepRuntimeSummary summary =
        StepRuntimeSummary.builder()
            .stepId("foo")
            .stepAttemptId(2)
            .stepInstanceUuid("bar")
            .stepName("step1")
            .stepInstanceId(123)
            .params(params)
            .build();
    summary.markTerminated(StepInstance.Status.SUCCEEDED, null);
    summary.addTimeline(TimelineLogEvent.info("hello"));

    var definition = new SignalDependenciesDefinition.SignalDependencyDefinition();
    definition.setName("signal_a");
    definition.setMatchParams(
        Map.of(
            "param_a",
            SignalMatchParamDef.builder()
                .operator(SignalOperator.EQUALS_TO)
                .param(StringParamDefinition.builder().name("param_a").value("test123").build())
                .build()));
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put("name", StringParamDefinition.builder().name("name").value("signal_a").build());
    paramDefMap.put(
        "param_a", StringParamDefinition.builder().name("param_a").value("test123").build());
    Map<String, Object> evaluatedResult = new HashMap<>();
    evaluatedResult.put("name", "signal_a");
    evaluatedResult.put("param_a", "test123");

    MapParameter mapParameter =
        MapParameter.builder()
            .value(paramDefMap)
            .evaluatedResult(evaluatedResult)
            .evaluatedTime(12345L)
            .build();

    summary.initializeSignalDependencies(List.of(definition), List.of(mapParameter));
    assertNotNull(summary.getSignalDependencies());

    assertEquals(
        StepDependencyMatchStatus.PENDING,
        summary.getSignalDependencies().getDependencies().getFirst().getStatus());
  }

  @Test
  public void testNoChangeMerge() throws Exception {
    StepRuntimeSummary summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-1.json", StepRuntimeSummary.class);
    assertTrue(summary.isSynced());
    summary.mergeRuntimeUpdate(null, null);
    assertTrue(summary.isSynced());

    Map<String, Artifact> artifacts = new LinkedHashMap<>();
    DefaultArtifact artifact1 = new DefaultArtifact();
    artifact1.add("value", 1L);
    artifact1.add("foo", "bar");
    artifacts.put("artifact1", artifact1);
    summary.mergeRuntimeUpdate(null, artifacts);
    assertTrue(summary.isSynced());

    SubworkflowArtifact artifact2 = new SubworkflowArtifact();
    artifact2.setSubworkflowId("test-dag");
    artifact2.setSubworkflowVersionId(1L);
    artifact2.setSubworkflowInstanceId(1);
    artifact2.setSubworkflowRunId(1);
    artifact2.setSubworkflowUuid("foo-bar");
    artifact2.setSubworkflowOverview(
        WorkflowRuntimeOverview.of(
            1L,
            singletonEnumMap(StepInstance.Status.SUCCEEDED, WorkflowStepStatusSummary.of(1L)),
            null));
    artifacts.put(artifact2.getType().key(), artifact2);
    summary.mergeRuntimeUpdate(null, artifacts);
    assertTrue(summary.isSynced());

    summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-3.json", StepRuntimeSummary.class);
    artifacts.clear();
    ForeachArtifact artifact3 = new ForeachArtifact();
    artifact3.setForeachWorkflowId("inline-wf");
    artifact3.setForeachIdentity("foo");
    artifact3.setTotalLoopCount(10);
    artifact3.setNextLoopIndex(0);
    artifact3.setForeachOverview(new ForeachStepOverview());
    artifact3.getForeachOverview().setStats(new EnumMap<>(WorkflowInstance.Status.class));
    artifact3.getForeachOverview().setCheckpoint(6L);
    artifact3.getForeachOverview().getStats().put(WorkflowInstance.Status.CREATED, 5L);
    artifact3.getForeachOverview().getStats().put(WorkflowInstance.Status.SUCCEEDED, 1L);
    artifacts.put(artifact3.getType().key(), artifact3);
    summary.mergeRuntimeUpdate(null, artifacts);
    assertTrue(summary.isSynced());
  }

  @Test
  public void testInvalidTerminate() throws Exception {
    StepRuntimeSummary summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-1.json", StepRuntimeSummary.class);
    AssertHelper.assertThrows(
        "Cannot terminate the step",
        IllegalArgumentException.class,
        "Cannot terminate step [foo][2][bar] to a non-terminal state [RUNNING]",
        () -> summary.markTerminated(StepInstance.Status.RUNNING, null));
  }

  @Test
  public void testMergeTimeline() throws Exception {
    StepRuntimeSummary summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-1.json", StepRuntimeSummary.class);
    assertTrue(summary.isSynced());
    TimelineEvent curEvent = summary.getTimeline().getTimelineEvents().getFirst();
    TimelineEvent newEvent = TimelineLogEvent.builder().message("world").build();
    summary.mergeRuntimeUpdate(Collections.singletonList(newEvent), null);
    assertFalse(summary.isSynced());
    assertEquals(new Timeline(Arrays.asList(curEvent, newEvent)), summary.getTimeline());
  }

  @Test
  public void testAddDuplicateTimelineEvents() throws Exception {
    StepRuntimeSummary summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-1.json", StepRuntimeSummary.class);
    TimelineEvent event = TimelineLogEvent.info("hello world");
    summary.addTimeline(event);
    summary.addTimeline(TimelineLogEvent.info("hello world"));
    summary.addTimeline(TimelineLogEvent.info("hello world"));
    summary.addTimeline(TimelineLogEvent.info("hello world"));
    assertEquals(2, summary.getTimeline().getTimelineEvents().size());
    assertEquals(event, summary.getTimeline().getTimelineEvents().get(1));
  }

  @Test
  public void testShouldIgnoreFailureMode() throws Exception {
    StepRuntimeSummary summary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-1.json", StepRuntimeSummary.class);
    assertFalse(summary.isIgnoreFailureMode());

    StepAction stepAction = mock(StepAction.class);
    WorkflowSummary workflowSummary = mock(WorkflowSummary.class);

    when(stepAction.getAction()).thenReturn(Actions.StepInstanceAction.KILL);
    when(stepAction.isWorkflowAction()).thenReturn(true);
    summary.configIgnoreFailureMode(stepAction, workflowSummary);
    assertTrue(summary.isIgnoreFailureMode());

    when(stepAction.getAction()).thenReturn(Actions.StepInstanceAction.KILL);
    when(stepAction.isWorkflowAction()).thenReturn(false);
    when(stepAction.getWorkflowId()).thenReturn("wf1");
    when(workflowSummary.getWorkflowId()).thenReturn("wf2");
    summary.configIgnoreFailureMode(stepAction, workflowSummary);
    assertTrue(summary.isIgnoreFailureMode());

    when(stepAction.getAction()).thenReturn(Actions.StepInstanceAction.KILL);
    when(stepAction.isWorkflowAction()).thenReturn(false);
    when(stepAction.getWorkflowId()).thenReturn("wf1");
    when(stepAction.getWorkflowInstanceId()).thenReturn(123L);
    when(stepAction.getWorkflowRunId()).thenReturn(2L);
    when(stepAction.getStepId()).thenReturn("foo");
    when(workflowSummary.getWorkflowId()).thenReturn("wf1");
    when(workflowSummary.getWorkflowInstanceId()).thenReturn(123L);
    when(workflowSummary.getWorkflowRunId()).thenReturn(2L);
    summary.configIgnoreFailureMode(stepAction, workflowSummary);
    assertFalse(summary.isIgnoreFailureMode());

    when(stepAction.getAction()).thenReturn(Actions.StepInstanceAction.STOP);
    summary.configIgnoreFailureMode(stepAction, workflowSummary);
    assertFalse(summary.isIgnoreFailureMode());

    when(stepAction.getAction()).thenReturn(Actions.StepInstanceAction.SKIP);
    summary.configIgnoreFailureMode(stepAction, workflowSummary);
    assertFalse(summary.isIgnoreFailureMode());
  }

  @Test
  public void testTracingOnStateTransition() {
    MaestroTracingContext tracingContext =
        MaestroTracingContext.builder().traceIdHigh(1L).traceIdLow(2L).spanId(3L).build();
    StepRuntimeSummary summary =
        StepRuntimeSummary.builder()
            .stepId("foo")
            .stepAttemptId(2)
            .stepInstanceUuid("bar")
            .stepName("step1")
            .stepInstanceId(123)
            .stepRetry(StepInstance.StepRetry.from(Defaults.DEFAULT_RETRY_POLICY))
            .tracingContext(tracingContext)
            .build();

    MaestroTracingManager tracingManager = mock(MaestroTracingManager.class);
    summary.markCreated(tracingManager);
    verify(tracingManager, times(1)).handleStepStatus(tracingContext, StepInstance.Status.CREATED);

    summary.markInitialized(tracingManager);
    verify(tracingManager, times(1))
        .handleStepStatus(tracingContext, StepInstance.Status.INITIALIZED);

    Exception ex = new RuntimeException("test ex");
    summary.markInternalError(ex, tracingManager);
    verify(tracingManager, times(1))
        .handleStepStatus(tracingContext, StepInstance.Status.INTERNALLY_FAILED, ex);

    summary.markTerminated(StepInstance.Status.PLATFORM_FAILED, tracingManager);
    verify(tracingManager, times(1))
        .handleStepStatus(tracingContext, StepInstance.Status.PLATFORM_FAILED);

    summary.markTerminated(StepInstance.Status.UNSATISFIED, tracingManager);
    verify(tracingManager, times(1))
        .handleStepStatus(tracingContext, StepInstance.Status.UNSATISFIED);

    summary.markPaused(tracingManager);
    verify(tracingManager, times(1)).handleStepStatus(tracingContext, StepInstance.Status.PAUSED);

    summary.markWaitSignal(tracingManager);
    verify(tracingManager, times(1))
        .handleStepStatus(tracingContext, StepInstance.Status.WAITING_FOR_SIGNALS);

    summary.markEvaluateParam(tracingManager);
    verify(tracingManager, times(1))
        .handleStepStatus(tracingContext, StepInstance.Status.EVALUATING_PARAMS);

    summary.markWaitPermit(tracingManager);
    verify(tracingManager, times(1))
        .handleStepStatus(tracingContext, StepInstance.Status.WAITING_FOR_PERMITS);

    summary.markStarting(tracingManager);
    verify(tracingManager, times(1)).handleStepStatus(tracingContext, StepInstance.Status.STARTING);

    summary.markExecuting(tracingManager);
    verify(tracingManager, times(1)).handleStepStatus(tracingContext, StepInstance.Status.RUNNING);

    summary.markFinishing(tracingManager);
    verify(tracingManager, times(1))
        .handleStepStatus(tracingContext, StepInstance.Status.FINISHING);
  }
}
