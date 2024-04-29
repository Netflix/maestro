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
package com.netflix.maestro.engine.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.api.RestartPolicy;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowInstanceAggregatedInfo;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mock;

public class AggregatedViewHelperTest extends MaestroEngineBaseTest {
  @Mock private MaestroWorkflowInstanceDao workflowInstanceDao;

  @Test
  public void testAggregatedViewSimple() {
    WorkflowInstance run1 =
        getGenericWorkflowInstance(
            1, WorkflowInstance.Status.SUCCEEDED, RunPolicy.START_FRESH_NEW_RUN, null);

    Workflow runtimeWorkflow = mock(Workflow.class);

    Map<String, StepRuntimeState> decodedOverview = new LinkedHashMap<>();
    decodedOverview.put("step1", generateStepState(StepInstance.Status.SUCCEEDED, 1L, 2L));
    decodedOverview.put("step2", generateStepState(StepInstance.Status.SUCCEEDED, 3L, 4L));
    decodedOverview.put("step3", generateStepState(StepInstance.Status.SUCCEEDED, 5L, 6L));

    WorkflowRuntimeOverview overview = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview).when(overview).decodeStepOverview(run1.getRuntimeDag());
    run1.setRuntimeOverview(overview);
    run1.setRuntimeWorkflow(runtimeWorkflow);

    WorkflowInstanceAggregatedInfo aggregated =
        AggregatedViewHelper.computeAggregatedView(run1, false);

    assertEquals(
        decodedOverview.get("step1").getStatus(),
        aggregated.getStepAggregatedViews().get("step1").getStatus());
    assertEquals(
        decodedOverview.get("step2").getStatus(),
        aggregated.getStepAggregatedViews().get("step2").getStatus());
    assertEquals(
        decodedOverview.get("step3").getStatus(),
        aggregated.getStepAggregatedViews().get("step3").getStatus());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, aggregated.getWorkflowInstanceStatus());

    WorkflowInstance run2 =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.SUCCEEDED,
            RunPolicy.RESTART_FROM_BEGINNING,
            RestartPolicy.RESTART_FROM_BEGINNING);

    Map<String, StepRuntimeState> decodedOverview2 = new LinkedHashMap<>();
    decodedOverview2.put("step1", generateStepState(StepInstance.Status.SUCCEEDED, 7L, 8L));
    decodedOverview2.put("step2", generateStepState(StepInstance.Status.SUCCEEDED, 9L, 10L));
    decodedOverview2.put("step3", generateStepState(StepInstance.Status.SUCCEEDED, 11L, 12L));

    doReturn(run1)
        .when(workflowInstanceDao)
        .getWorkflowInstanceRun(run2.getWorkflowId(), run2.getWorkflowInstanceId(), 1L);
    run2.setAggregatedInfo(AggregatedViewHelper.computeAggregatedView(run1, true));

    assertEquals(3, run2.getAggregatedInfo().getStepAggregatedViews().size());
    assertEquals(
        1L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(
        3L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(
        5L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step3").getStartTime().longValue());

    WorkflowRuntimeOverview wro2 = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview2).when(wro2).decodeStepOverview(run2.getRuntimeDag());
    run2.setRuntimeOverview(wro2);
    run2.setRuntimeWorkflow(runtimeWorkflow);

    WorkflowInstanceAggregatedInfo aggregated2 =
        AggregatedViewHelper.computeAggregatedView(run2, false);

    assertEquals(
        2L, aggregated2.getStepAggregatedViews().get("step1").getWorkflowRunId().longValue());
    assertEquals(
        2L, aggregated2.getStepAggregatedViews().get("step2").getWorkflowRunId().longValue());
    assertEquals(
        2L, aggregated2.getStepAggregatedViews().get("step3").getWorkflowRunId().longValue());
    assertEquals(7L, aggregated2.getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(9L, aggregated2.getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(11L, aggregated2.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, aggregated2.getWorkflowInstanceStatus());
  }

  @Test
  public void testAggregatedViewFailed() {
    WorkflowInstance run1 =
        getGenericWorkflowInstance(
            1, WorkflowInstance.Status.FAILED, RunPolicy.START_FRESH_NEW_RUN, null);

    Workflow runtimeWorkflow = mock(Workflow.class);

    Map<String, StepRuntimeState> decodedOverview = new LinkedHashMap<>();
    decodedOverview.put("step1", generateStepState(StepInstance.Status.SUCCEEDED, 1L, 2L));
    decodedOverview.put("step2", generateStepState(StepInstance.Status.SUCCEEDED, 3L, 4L));
    decodedOverview.put("step3", generateStepState(StepInstance.Status.FATALLY_FAILED, 5L, 6L));

    WorkflowRuntimeOverview wro = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview).when(wro).decodeStepOverview(run1.getRuntimeDag());
    run1.setRuntimeOverview(wro);
    run1.setRuntimeWorkflow(runtimeWorkflow);

    WorkflowInstanceAggregatedInfo aggregated =
        AggregatedViewHelper.computeAggregatedView(run1, false);

    assertEquals(1L, aggregated.getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(3L, aggregated.getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(5L, aggregated.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(WorkflowInstance.Status.FAILED, aggregated.getWorkflowInstanceStatus());

    WorkflowInstance run2 =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.SUCCEEDED,
            RunPolicy.RESTART_FROM_INCOMPLETE,
            RestartPolicy.RESTART_FROM_INCOMPLETE);

    Map<String, StepRuntimeState> decodedOverview2 = new LinkedHashMap<>();
    decodedOverview2.put("step3", generateStepState(StepInstance.Status.SUCCEEDED, 11L, 12L));

    Map<String, StepTransition> run2Dag = new LinkedHashMap<>();
    run2Dag.put("step3", new StepTransition());
    run2.setRuntimeDag(run2Dag);

    doReturn(run1)
        .when(workflowInstanceDao)
        .getWorkflowInstanceRun(run2.getWorkflowId(), run2.getWorkflowInstanceId(), 1L);
    run2.setAggregatedInfo(AggregatedViewHelper.computeAggregatedView(run1, false));

    assertEquals(3, run2.getAggregatedInfo().getStepAggregatedViews().size());
    assertEquals(
        1L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(
        3L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(
        5L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step3").getStartTime().longValue());

    WorkflowRuntimeOverview wro2 = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview2).when(wro2).decodeStepOverview(run2.getRuntimeDag());
    run2.setRuntimeOverview(wro2);
    run2.setRuntimeWorkflow(runtimeWorkflow);

    WorkflowInstanceAggregatedInfo aggregated2 =
        AggregatedViewHelper.computeAggregatedView(run2, false);

    assertEquals(3, aggregated2.getStepAggregatedViews().size());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step1").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step2").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step3").getStatus());
    assertEquals(1L, aggregated2.getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(3L, aggregated2.getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(11L, aggregated2.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, aggregated2.getWorkflowInstanceStatus());
  }

  @Test
  public void testComputeAggregatedViewWithoutRuntimeOverview() {
    WorkflowInstance instance =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.STOPPED,
            RunPolicy.RESTART_FROM_BEGINNING,
            RestartPolicy.RESTART_FROM_BEGINNING);
    instance.setAggregatedInfo(new WorkflowInstanceAggregatedInfo());
    instance.getAggregatedInfo().setStepAggregatedViews(Collections.emptyMap());
    instance.getAggregatedInfo().setWorkflowInstanceStatus(WorkflowInstance.Status.SUCCEEDED);

    Workflow runtimeWorkflow = mock(Workflow.class);
    instance.setRuntimeWorkflow(runtimeWorkflow);
    Step step1 = mock(Step.class);
    when(step1.getId()).thenReturn("step1");
    Step step2 = mock(Step.class);
    when(step2.getId()).thenReturn("step2");
    Step step3 = mock(Step.class);
    when(step3.getId()).thenReturn("step3");
    when(runtimeWorkflow.getSteps()).thenReturn(Arrays.asList(step1, step2, step3));

    WorkflowInstanceAggregatedInfo aggregated =
        AggregatedViewHelper.computeAggregatedView(instance, true);

    assertEquals(
        StepInstance.Status.NOT_CREATED,
        aggregated.getStepAggregatedViews().get("step1").getStatus());
    assertEquals(
        StepInstance.Status.NOT_CREATED,
        aggregated.getStepAggregatedViews().get("step2").getStatus());
    assertEquals(
        StepInstance.Status.NOT_CREATED,
        aggregated.getStepAggregatedViews().get("step3").getStatus());
    assertEquals(WorkflowInstance.Status.STOPPED, aggregated.getWorkflowInstanceStatus());
  }

  @Test
  public void testComputeAggregatedViewForEmptyWorkflow() {
    WorkflowInstance instance =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.SUCCEEDED,
            RunPolicy.RESTART_FROM_BEGINNING,
            RestartPolicy.RESTART_FROM_BEGINNING);
    instance.setAggregatedInfo(new WorkflowInstanceAggregatedInfo());
    instance.getAggregatedInfo().setStepAggregatedViews(Collections.emptyMap());
    instance.getAggregatedInfo().setWorkflowInstanceStatus(WorkflowInstance.Status.STOPPED);

    Workflow runtimeWorkflow = mock(Workflow.class);
    instance.setRuntimeWorkflow(runtimeWorkflow);
    when(runtimeWorkflow.getSteps()).thenReturn(Collections.emptyList());

    WorkflowInstanceAggregatedInfo aggregated =
        AggregatedViewHelper.computeAggregatedView(instance, false);

    assertTrue(aggregated.getStepAggregatedViews().isEmpty());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, aggregated.getWorkflowInstanceStatus());
  }

  @Test
  public void testAggregatedViewSomeNotStarted() {
    WorkflowInstance run1 =
        getGenericWorkflowInstance(
            1, WorkflowInstance.Status.FAILED, RunPolicy.START_FRESH_NEW_RUN, null);

    Workflow runtimeWorkflow = mock(Workflow.class);

    Map<String, StepRuntimeState> decodedOverview = new LinkedHashMap<>();
    decodedOverview.put("step1", generateStepState(StepInstance.Status.SUCCEEDED, 1L, 2L));
    decodedOverview.put("step2", generateStepState(StepInstance.Status.SUCCEEDED, 3L, 4L));
    decodedOverview.put("step3", generateStepState(StepInstance.Status.FATALLY_FAILED, 5L, 6L));
    decodedOverview.put("step4", generateStepState(StepInstance.Status.NOT_CREATED, null, null));
    decodedOverview.put("step5", generateStepState(StepInstance.Status.NOT_CREATED, null, null));

    WorkflowRuntimeOverview wro = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview).when(wro).decodeStepOverview(run1.getRuntimeDag());
    run1.setRuntimeOverview(wro);
    run1.setRuntimeWorkflow(runtimeWorkflow);
    run1.getRuntimeDag().put("step4", new StepTransition());
    run1.getRuntimeDag().put("step5", new StepTransition());

    WorkflowInstanceAggregatedInfo aggregated =
        AggregatedViewHelper.computeAggregatedView(run1, false);

    assertEquals(1L, aggregated.getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(3L, aggregated.getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(5L, aggregated.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(WorkflowInstance.Status.FAILED, aggregated.getWorkflowInstanceStatus());

    WorkflowInstance run2 =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.SUCCEEDED,
            RunPolicy.RESTART_FROM_INCOMPLETE,
            RestartPolicy.RESTART_FROM_INCOMPLETE);

    Map<String, StepRuntimeState> decodedOverview2 = new LinkedHashMap<>();
    decodedOverview2.put("step3", generateStepState(StepInstance.Status.SUCCEEDED, 11L, 12L));
    decodedOverview2.put("step4", generateStepState(StepInstance.Status.SUCCEEDED, 14L, 15L));
    decodedOverview2.put("step5", generateStepState(StepInstance.Status.SUCCEEDED, 16L, 17L));

    Map<String, StepTransition> run2Dag = new LinkedHashMap<>();
    run2Dag.put("step3", new StepTransition());
    run2Dag.put("step4", new StepTransition());
    run2Dag.put("step5", new StepTransition());
    run2.setRuntimeDag(run2Dag);

    doReturn(run1)
        .when(workflowInstanceDao)
        .getWorkflowInstanceRun(run2.getWorkflowId(), run2.getWorkflowInstanceId(), 1L);
    run2.setAggregatedInfo(AggregatedViewHelper.computeAggregatedView(run1, false));

    assertEquals(5, run2.getAggregatedInfo().getStepAggregatedViews().size());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step1").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step2").getStatus());
    assertEquals(
        StepInstance.Status.FATALLY_FAILED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step3").getStatus());
    assertEquals(
        StepInstance.Status.NOT_CREATED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step4").getStatus());
    assertEquals(
        StepInstance.Status.NOT_CREATED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step5").getStatus());
    assertEquals(
        1L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(
        3L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(
        5L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step3").getStartTime().longValue());

    WorkflowRuntimeOverview wro2 = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview2).when(wro2).decodeStepOverview(run2.getRuntimeDag());
    run2.setRuntimeOverview(wro2);
    run2.setRuntimeWorkflow(runtimeWorkflow);

    WorkflowInstanceAggregatedInfo aggregated2 =
        AggregatedViewHelper.computeAggregatedView(run2, false);

    assertEquals(5, aggregated2.getStepAggregatedViews().size());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step1").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step2").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step3").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step4").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step5").getStatus());
    assertEquals(1L, aggregated2.getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(3L, aggregated2.getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(11L, aggregated2.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(14L, aggregated2.getStepAggregatedViews().get("step4").getStartTime().longValue());
    assertEquals(16L, aggregated2.getStepAggregatedViews().get("step5").getStartTime().longValue());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, aggregated2.getWorkflowInstanceStatus());
  }

  @Test
  public void testAggregatedViewDoublePreviouslySucceeded() {
    WorkflowInstance run1 =
        getGenericWorkflowInstance(
            1, WorkflowInstance.Status.SUCCEEDED, RunPolicy.START_FRESH_NEW_RUN, null);

    Workflow runtimeWorkflow = mock(Workflow.class);

    Map<String, StepRuntimeState> decodedOverview = new LinkedHashMap<>();
    decodedOverview.put("step1", generateStepState(StepInstance.Status.SUCCEEDED, 1L, 2L));
    decodedOverview.put("step2", generateStepState(StepInstance.Status.SUCCEEDED, 3L, 4L));
    decodedOverview.put("step3", generateStepState(StepInstance.Status.SUCCEEDED, 5L, 6L));
    decodedOverview.put("step4", generateStepState(StepInstance.Status.SUCCEEDED, 7L, 8L));
    decodedOverview.put("step5", generateStepState(StepInstance.Status.SUCCEEDED, 9L, 10L));

    WorkflowRuntimeOverview wro = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview).when(wro).decodeStepOverview(run1.getRuntimeDag());
    run1.setRuntimeOverview(wro);
    run1.setRuntimeWorkflow(runtimeWorkflow);
    run1.getRuntimeDag().put("step4", new StepTransition());
    run1.getRuntimeDag().put("step5", new StepTransition());

    WorkflowInstanceAggregatedInfo aggregated =
        AggregatedViewHelper.computeAggregatedView(run1, false);

    assertEquals(1L, aggregated.getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(3L, aggregated.getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(5L, aggregated.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(7L, aggregated.getStepAggregatedViews().get("step4").getStartTime().longValue());
    assertEquals(9L, aggregated.getStepAggregatedViews().get("step5").getStartTime().longValue());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, aggregated.getWorkflowInstanceStatus());

    WorkflowInstance run2 =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.FAILED,
            RunPolicy.RESTART_FROM_SPECIFIC,
            RestartPolicy.RESTART_FROM_BEGINNING);

    Map<String, StepRuntimeState> decodedOverview2 = new LinkedHashMap<>();
    decodedOverview2.put("step3", generateStepState(StepInstance.Status.FATALLY_FAILED, 11L, 12L));
    decodedOverview2.put("step4", generateStepState(StepInstance.Status.NOT_CREATED, 14L, 15L));
    decodedOverview2.put("step5", generateStepState(StepInstance.Status.NOT_CREATED, 16L, 17L));

    Map<String, StepTransition> run2Dag = new LinkedHashMap<>();
    run2Dag.put("step3", new StepTransition());
    run2Dag.put("step4", new StepTransition());
    run2Dag.put("step5", new StepTransition());
    run2.setRuntimeDag(run2Dag);

    doReturn(run1)
        .when(workflowInstanceDao)
        .getWorkflowInstanceRun(run2.getWorkflowId(), run2.getWorkflowInstanceId(), 1L);
    run2.setAggregatedInfo(AggregatedViewHelper.computeAggregatedView(run1, false));

    assertEquals(5, run2.getAggregatedInfo().getStepAggregatedViews().size());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step1").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step2").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step3").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step4").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step5").getStatus());
    assertEquals(
        1L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(
        3L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(5L, aggregated.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(7L, aggregated.getStepAggregatedViews().get("step4").getStartTime().longValue());
    assertEquals(9L, aggregated.getStepAggregatedViews().get("step5").getStartTime().longValue());

    WorkflowRuntimeOverview wro2 = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview2).when(wro2).decodeStepOverview(run2.getRuntimeDag());
    run2.setRuntimeOverview(wro2);
    run2.setRuntimeWorkflow(runtimeWorkflow);

    WorkflowInstanceAggregatedInfo aggregated2 =
        AggregatedViewHelper.computeAggregatedView(run2, false);

    assertEquals(5, aggregated2.getStepAggregatedViews().size());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step1").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step2").getStatus());
    assertEquals(
        StepInstance.Status.FATALLY_FAILED,
        aggregated2.getStepAggregatedViews().get("step3").getStatus());
    assertEquals(
        StepInstance.Status.NOT_CREATED,
        aggregated2.getStepAggregatedViews().get("step4").getStatus());
    assertEquals(
        StepInstance.Status.NOT_CREATED,
        aggregated2.getStepAggregatedViews().get("step5").getStatus());
    assertEquals(1L, aggregated2.getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(3L, aggregated2.getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(11L, aggregated2.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(14L, aggregated2.getStepAggregatedViews().get("step4").getStartTime().longValue());
    assertEquals(16L, aggregated2.getStepAggregatedViews().get("step5").getStartTime().longValue());
    assertEquals(WorkflowInstance.Status.FAILED, aggregated2.getWorkflowInstanceStatus());
  }

  @Test
  public void testAggregatedViewWithTwoBranchesFailed() {
    WorkflowInstance run1 =
        getGenericWorkflowInstance(
            1, WorkflowInstance.Status.FAILED, RunPolicy.START_FRESH_NEW_RUN, null);

    Workflow runtimeWorkflow = mock(Workflow.class);

    Map<String, StepRuntimeState> decodedOverview = new LinkedHashMap<>();
    decodedOverview.put("step1", generateStepState(StepInstance.Status.SUCCEEDED, 1L, 2L));
    decodedOverview.put("step2", generateStepState(StepInstance.Status.FATALLY_FAILED, 3L, 4L));
    decodedOverview.put("step3", generateStepState(StepInstance.Status.FATALLY_FAILED, 5L, 6L));

    WorkflowRuntimeOverview overview = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview).when(overview).decodeStepOverview(run1.getRuntimeDag());
    run1.setRuntimeOverview(overview);
    run1.setRuntimeWorkflow(runtimeWorkflow);

    WorkflowInstanceAggregatedInfo aggregated =
        AggregatedViewHelper.computeAggregatedView(run1, false);

    assertEquals(1L, aggregated.getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(3L, aggregated.getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(5L, aggregated.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(WorkflowInstance.Status.FAILED, aggregated.getWorkflowInstanceStatus());

    WorkflowInstance run2 =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.SUCCEEDED,
            RunPolicy.RESTART_FROM_SPECIFIC,
            RestartPolicy.RESTART_FROM_BEGINNING);
    RestartConfig config =
        RestartConfig.builder()
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .addRestartNode(run2.getWorkflowId(), run2.getWorkflowInstanceId(), "step2")
            .build();
    run2.getRunConfig().setRestartConfig(config);

    Map<String, StepRuntimeState> decodedOverview2 = new LinkedHashMap<>();
    decodedOverview2.put("step2", generateStepState(StepInstance.Status.SUCCEEDED, 11L, 12L));

    Map<String, StepTransition> run2Dag = new LinkedHashMap<>();
    run2Dag.put("step2", new StepTransition());
    run2Dag.put("step3", new StepTransition());
    run2.setRuntimeDag(run2Dag);

    doReturn(run1)
        .when(workflowInstanceDao)
        .getWorkflowInstanceRun(run2.getWorkflowId(), run2.getWorkflowInstanceId(), 1L);
    run2.setAggregatedInfo(AggregatedViewHelper.computeAggregatedView(run1, false));

    assertEquals(3, run2.getAggregatedInfo().getStepAggregatedViews().size());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step1").getStatus());
    assertEquals(
        StepInstance.Status.FATALLY_FAILED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step2").getStatus());
    assertEquals(
        StepInstance.Status.FATALLY_FAILED,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step3").getStatus());
    assertEquals(
        1L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(
        3L,
        run2.getAggregatedInfo().getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(5L, aggregated.getStepAggregatedViews().get("step3").getStartTime().longValue());

    WorkflowRuntimeOverview wro2 = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview2).when(wro2).decodeStepOverview(run2.getRuntimeDag());
    run2.setRuntimeOverview(wro2);
    run2.setRuntimeWorkflow(runtimeWorkflow);

    WorkflowInstanceAggregatedInfo aggregated2 =
        AggregatedViewHelper.computeAggregatedView(run2, false);

    assertEquals(3, aggregated2.getStepAggregatedViews().size());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step1").getStatus());
    assertEquals(
        StepInstance.Status.SUCCEEDED,
        aggregated2.getStepAggregatedViews().get("step2").getStatus());
    assertEquals(
        StepInstance.Status.FATALLY_FAILED,
        aggregated2.getStepAggregatedViews().get("step3").getStatus());
    assertEquals(1L, aggregated2.getStepAggregatedViews().get("step1").getStartTime().longValue());
    assertEquals(11L, aggregated2.getStepAggregatedViews().get("step2").getStartTime().longValue());
    assertEquals(5L, aggregated2.getStepAggregatedViews().get("step3").getStartTime().longValue());
    assertEquals(WorkflowInstance.Status.FAILED, aggregated2.getWorkflowInstanceStatus());
  }

  @Test
  public void testStatusAggregation() {
    WorkflowInstance run2 =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.SUCCEEDED,
            RunPolicy.START_CUSTOMIZED_RUN,
            RestartPolicy.RESTART_FROM_INCOMPLETE);

    WorkflowInstance run3 =
        getGenericWorkflowInstance(
            3,
            WorkflowInstance.Status.IN_PROGRESS,
            RunPolicy.START_CUSTOMIZED_RUN,
            RestartPolicy.RESTART_FROM_INCOMPLETE);

    WorkflowInstance run4 =
        getGenericWorkflowInstance(
            4,
            WorkflowInstance.Status.STOPPED,
            RunPolicy.START_CUSTOMIZED_RUN,
            RestartPolicy.RESTART_FROM_INCOMPLETE);

    WorkflowInstance run5 =
        getGenericWorkflowInstance(
            5,
            WorkflowInstance.Status.FAILED,
            RunPolicy.START_CUSTOMIZED_RUN,
            RestartPolicy.RESTART_FROM_INCOMPLETE);

    WorkflowInstanceAggregatedInfo aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("step1", generateStepAggregated(StepInstance.Status.SUCCEEDED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step2", generateStepAggregated(StepInstance.Status.SUCCEEDED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step3", generateStepAggregated(StepInstance.Status.SUCCEEDED, 11L, 12L));
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run2, aggregated);
    assertEquals(WorkflowInstance.Status.SUCCEEDED, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("step1", generateStepAggregated(StepInstance.Status.SUCCEEDED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step2", generateStepAggregated(StepInstance.Status.FATALLY_FAILED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step3", generateStepAggregated(StepInstance.Status.SUCCEEDED, 11L, 12L));
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run5, aggregated);
    assertEquals(WorkflowInstance.Status.FAILED, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("step1", generateStepAggregated(StepInstance.Status.NOT_CREATED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step2", generateStepAggregated(StepInstance.Status.NOT_CREATED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step3", generateStepAggregated(StepInstance.Status.NOT_CREATED, 11L, 12L));
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run3, aggregated);
    assertEquals(WorkflowInstance.Status.IN_PROGRESS, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("step1", generateStepAggregated(StepInstance.Status.SUCCEEDED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step2", generateStepAggregated(StepInstance.Status.NOT_CREATED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step3", generateStepAggregated(StepInstance.Status.NOT_CREATED, 11L, 12L));
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run3, aggregated);
    assertEquals(WorkflowInstance.Status.IN_PROGRESS, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("step1", generateStepAggregated(StepInstance.Status.INTERNALLY_FAILED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step2", generateStepAggregated(StepInstance.Status.NOT_CREATED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step3", generateStepAggregated(StepInstance.Status.NOT_CREATED, 11L, 12L));
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run5, aggregated);
    assertEquals(WorkflowInstance.Status.FAILED, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("step1", generateStepAggregated(StepInstance.Status.INTERNALLY_FAILED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step2", generateStepAggregated(StepInstance.Status.STOPPED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step3", generateStepAggregated(StepInstance.Status.STOPPED, 11L, 12L));
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run5, aggregated);
    assertEquals(WorkflowInstance.Status.FAILED, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("step1", generateStepAggregated(StepInstance.Status.SUCCEEDED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step2", generateStepAggregated(StepInstance.Status.STARTING, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step3", generateStepAggregated(StepInstance.Status.STOPPED, 11L, 12L));
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run3, aggregated);
    assertEquals(WorkflowInstance.Status.IN_PROGRESS, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("step1", generateStepAggregated(StepInstance.Status.STOPPED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step2", generateStepAggregated(StepInstance.Status.NOT_CREATED, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("step3", generateStepAggregated(StepInstance.Status.NOT_CREATED, 11L, 12L));
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run4, aggregated);
    assertEquals(WorkflowInstance.Status.STOPPED, aggregated.getWorkflowInstanceStatus());
  }

  @Test
  public void testAggregatedInstanceStatusWhenNoNeedToAggregateV2() {
    WorkflowInstanceAggregatedInfo aggregated = new WorkflowInstanceAggregatedInfo();
    WorkflowInstance run1 =
        getGenericWorkflowInstance(
            1,
            WorkflowInstance.Status.IN_PROGRESS,
            RunPolicy.START_FRESH_NEW_RUN,
            RestartPolicy.RESTART_FROM_BEGINNING);
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run1, aggregated);
    assertEquals(WorkflowInstance.Status.IN_PROGRESS, aggregated.getWorkflowInstanceStatus());

    WorkflowInstance run2RestartFromBeginning =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.SUCCEEDED,
            RunPolicy.RESTART_FROM_BEGINNING,
            RestartPolicy.RESTART_FROM_BEGINNING);
    aggregated = new WorkflowInstanceAggregatedInfo();
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(
        run2RestartFromBeginning, aggregated);
    assertEquals(WorkflowInstance.Status.SUCCEEDED, aggregated.getWorkflowInstanceStatus());

    WorkflowInstance run2Failed =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.FAILED,
            RunPolicy.RESTART_FROM_INCOMPLETE,
            RestartPolicy.RESTART_FROM_BEGINNING);
    aggregated = new WorkflowInstanceAggregatedInfo();
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run2Failed, aggregated);
    assertEquals(WorkflowInstance.Status.FAILED, aggregated.getWorkflowInstanceStatus());
  }

  @Test
  public void testAggregatedInstanceStatusFromAggregatedV2() {
    WorkflowInstance run1 =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.SUCCEEDED,
            RunPolicy.RESTART_FROM_INCOMPLETE,
            RestartPolicy.RESTART_FROM_INCOMPLETE);

    WorkflowInstanceAggregatedInfo aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("1", generateStepAggregated(StepInstance.Status.SUCCEEDED, 1L, 2L));
    aggregated
        .getStepAggregatedViews()
        .put("2", generateStepAggregated(StepInstance.Status.TIMED_OUT, 11L, 12L));
    aggregated
        .getStepAggregatedViews()
        .put("3", generateStepAggregated(StepInstance.Status.INTERNALLY_FAILED, 11L, 12L));

    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run1, aggregated);
    assertEquals(WorkflowInstance.Status.FAILED, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("1", generateStepAggregated(StepInstance.Status.SUCCEEDED, 1L, 2L));
    aggregated
        .getStepAggregatedViews()
        .put("2", generateStepAggregated(StepInstance.Status.STOPPED, 1L, 2L));
    aggregated
        .getStepAggregatedViews()
        .put("3", generateStepAggregated(StepInstance.Status.TIMED_OUT, 1L, 2L));

    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run1, aggregated);
    assertEquals(WorkflowInstance.Status.TIMED_OUT, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("1", generateStepAggregated(StepInstance.Status.SUCCEEDED, 1L, 2L));
    aggregated
        .getStepAggregatedViews()
        .put("2", generateStepAggregated(StepInstance.Status.SUCCEEDED, 1L, 2L));
    aggregated
        .getStepAggregatedViews()
        .put("3", generateStepAggregated(StepInstance.Status.STOPPED, 1L, 2L));
    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run1, aggregated);
    assertEquals(WorkflowInstance.Status.STOPPED, aggregated.getWorkflowInstanceStatus());

    aggregated = new WorkflowInstanceAggregatedInfo();
    aggregated
        .getStepAggregatedViews()
        .put("1", generateStepAggregated(StepInstance.Status.SUCCEEDED, 1L, 2L));
    aggregated
        .getStepAggregatedViews()
        .put("2", generateStepAggregated(StepInstance.Status.SUCCEEDED, 1L, 2L));
    aggregated
        .getStepAggregatedViews()
        .put("3", generateStepAggregated(StepInstance.Status.SUCCEEDED, 1L, 2L));

    AggregatedViewHelper.computeAndSetAggregatedInstanceStatus(run1, aggregated);
    assertEquals(WorkflowInstance.Status.SUCCEEDED, aggregated.getWorkflowInstanceStatus());
  }

  @Test
  public void testDeriveAggregatedStatus() {
    WorkflowInstance instance =
        getGenericWorkflowInstance(
            2,
            WorkflowInstance.Status.SUCCEEDED,
            RunPolicy.RESTART_FROM_SPECIFIC,
            RestartPolicy.RESTART_FROM_BEGINNING);
    instance.getRuntimeDag().remove("step1");

    MaestroWorkflowInstanceDao instanceDao = mock(MaestroWorkflowInstanceDao.class);
    when(instanceDao.getWorkflowInstanceRun(any(), anyLong(), anyLong())).thenReturn(instance);

    WorkflowSummary summary = mock(WorkflowSummary.class);
    when(summary.isFreshRun()).thenReturn(false);
    Workflow runtimeWorkflow = mock(Workflow.class);
    Step step1 = mock(Step.class);
    when(step1.getId()).thenReturn("step1");
    Step step2 = mock(Step.class);
    when(step2.getId()).thenReturn("step2");
    Step step3 = mock(Step.class);
    when(step3.getId()).thenReturn("step3");
    when(runtimeWorkflow.getSteps()).thenReturn(Arrays.asList(step1, step2, step3));
    instance.setRuntimeWorkflow(runtimeWorkflow);
    WorkflowInstanceAggregatedInfo baseline = new WorkflowInstanceAggregatedInfo();
    baseline.setWorkflowInstanceStatus(WorkflowInstance.Status.FAILED);
    baseline
        .getStepAggregatedViews()
        .put("step1", generateStepAggregated(StepInstance.Status.SUCCEEDED, 11L, 12L));
    baseline
        .getStepAggregatedViews()
        .put("step2", generateStepAggregated(StepInstance.Status.FATALLY_FAILED, 11L, 12L));
    baseline
        .getStepAggregatedViews()
        .put("step3", generateStepAggregated(StepInstance.Status.STOPPED, 11L, 12L));
    instance.setAggregatedInfo(baseline);

    Map<String, StepRuntimeState> decodedOverview = new LinkedHashMap<>();
    decodedOverview.put("step2", generateStepState(StepInstance.Status.SUCCEEDED, 11L, 12L));

    WorkflowRuntimeOverview overview = mock(WorkflowRuntimeOverview.class);
    doReturn(decodedOverview).when(overview).decodeStepOverview(instance.getRuntimeDag());

    WorkflowInstance.Status actual =
        AggregatedViewHelper.deriveAggregatedStatus(
            instanceDao, summary, WorkflowInstance.Status.FAILED, overview);
    assertEquals(WorkflowInstance.Status.FAILED, actual);
    verify(overview, times(0)).setRunStatus(any());

    actual =
        AggregatedViewHelper.deriveAggregatedStatus(
            instanceDao, summary, WorkflowInstance.Status.SUCCEEDED, overview);
    assertEquals(WorkflowInstance.Status.FAILED, actual);
    verify(overview, times(1)).setRunStatus(WorkflowInstance.Status.SUCCEEDED);
  }

  /** Below are helper methods for tests to create sample WorkflowInstance and StepInstance. */
  private WorkflowInstance getGenericWorkflowInstance(
      long runId,
      WorkflowInstance.Status status,
      RunPolicy runPolicy,
      RestartPolicy restartPolicy) {
    WorkflowInstance wi = new WorkflowInstance();
    wi.setWorkflowId("testWorkflowId");
    wi.setWorkflowInstanceId(1);
    wi.setStatus(status);
    if (runPolicy != null) {
      RunConfig rg = new RunConfig();
      rg.setPolicy(runPolicy);
      wi.setRunConfig(rg);
    }
    if (restartPolicy != null) {
      if (wi.getRunConfig() == null) {
        RunConfig rg = new RunConfig();
        RestartConfig rc = RestartConfig.builder().setRestartPolicy(restartPolicy).build();
        rg.setRestartConfig(rc);
        wi.setRunConfig(rg);
      }
    }
    wi.setWorkflowRunId(runId);
    Map<String, StepTransition> runDag = new LinkedHashMap<>();
    runDag.put("step1", new StepTransition());
    runDag.put("step2", new StepTransition());
    runDag.put("step3", new StepTransition());
    wi.setRuntimeDag(runDag);
    return wi;
  }

  private StepRuntimeState generateStepState(
      StepInstance.Status status, Long startTime, Long endTime) {
    StepRuntimeState srs = new StepRuntimeState();
    srs.setStatus(status);
    srs.setStartTime(startTime);
    srs.setEndTime(endTime);
    return srs;
  }

  private StepAggregatedView generateStepAggregated(
      StepInstance.Status status, Long startTime, Long endTime) {
    return StepAggregatedView.builder()
        .workflowRunId(1L)
        .status(status)
        .startTime(startTime)
        .endTime(endTime)
        .build();
  }
}
