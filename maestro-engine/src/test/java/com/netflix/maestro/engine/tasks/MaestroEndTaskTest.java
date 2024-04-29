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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowRuntimeSummary;
import com.netflix.maestro.engine.jobevents.TerminateInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.utils.RollupAggregationHelper;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.SubworkflowArtifact;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.RunConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroEndTaskTest extends MaestroEngineBaseTest {

  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private MaestroJobEventPublisher publisher;

  private MaestroEndTask endTask;
  private Workflow workflow;
  private Task testTask;
  private RollupAggregationHelper rollupAggregationHelper;

  @Before
  public void setup() throws IOException {
    WorkflowInstance workflowInstance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created-foreach-subworkflow-1.json",
            WorkflowInstance.class);
    workflowInstance.setRunConfig(new RunConfig());
    workflowInstance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    workflowInstance.setWorkflowId("testWorkflowId");
    doReturn(workflowInstance).when(instanceDao).getWorkflowInstanceRun("testWorkflowId", 123L, 1L);
    rollupAggregationHelper = spy(new RollupAggregationHelper(stepInstanceDao));
    endTask =
        new MaestroEndTask(instanceDao, publisher, MAPPER, rollupAggregationHelper, metricRepo);

    testTask = new Task();
    testTask.setTaskType("MAESTRO_TASK");
    testTask.setStatus(Task.Status.IN_PROGRESS);
    testTask.setTaskId("test-task-id");
    testTask.setReferenceTaskName("job1");
    testTask.setWorkflowTask(new WorkflowTask());
    testTask.setInputData(
        Collections.singletonMap("maestroTask", Collections.singletonList("job1")));

    workflow = new Workflow();
    workflow.setWorkflowId("testWorkflowId");
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
    WorkflowDef def = new WorkflowDef();
    workflow.setWorkflowDefinition(def);
    Map<String, Object> summary = new HashMap<>();
    summary.put("workflow_id", "testWorkflowId");
    summary.put("workflow_instance_id", 123);
    summary.put("workflow_run_id", 1);
    summary.put("runtime_dag", singletonMap("job1", Collections.emptyMap()));
    summary.put("initiator", twoItemMap("type", "MANUAL", "user", "tester"));
    workflow.setInput(Collections.singletonMap("maestro_workflow_summary", summary));
    workflow.setTasks(Collections.singletonList(testTask));
  }

  @Test
  public void testExecuteReachLeafStepLimit() {
    StepRuntimeState runtimeState = new StepRuntimeState();
    runtimeState.setStatus(StepInstance.Status.RUNNING);
    WorkflowRollupOverview rollup = new WorkflowRollupOverview();
    rollup.setTotalLeafCount(Constants.TOTAL_LEAF_STEP_COUNT_LIMIT + 1);
    SubworkflowArtifact artifact = new SubworkflowArtifact();
    artifact.setSubworkflowOverview(
        WorkflowRuntimeOverview.of(1, new EnumMap<>(StepInstance.Status.class), rollup));
    WorkflowRuntimeSummary runtimeSummary = new WorkflowRuntimeSummary();
    runtimeSummary.setInstanceStatus(WorkflowInstance.Status.IN_PROGRESS);
    runtimeSummary.setRollupBase(rollup);
    testTask.setOutputData(
        twoItemMap(
            "maestro_step_runtime_summary",
            StepRuntimeSummary.builder()
                .stepId("job1")
                .stepAttemptId(2)
                .stepInstanceUuid("bar")
                .stepName("step1")
                .stepInstanceId(123)
                .type(StepType.SUBWORKFLOW)
                .runtimeState(runtimeState)
                .artifacts(Collections.singletonMap("maestro_subworkflow", artifact))
                .build(),
            "maestro_workflow_runtime_summary",
            runtimeSummary));

    endTask.execute(workflow, testTask, null);

    Assert.assertEquals(
        rollup.getTotalLeafCount() * 2,
        ((WorkflowRuntimeSummary) testTask.getOutputData().get("maestro_workflow_runtime_summary"))
            .getRuntimeOverview()
            .getRollupOverview()
            .getTotalLeafCount());

    verify(publisher, times(1)).publish(any(TerminateInstancesJobEvent.class));
  }

  @Test
  public void testMarkMaestroWorkflowStarted() {
    workflow.setEvent("234567");
    testTask.setReferenceTaskName("maestro_start");
    testTask.setStartTime(123456L);
    testTask.setOutputData(
        twoItemMap(
            "maestro_step_runtime_summary",
            StepRuntimeSummary.builder()
                .stepId("job1")
                .stepAttemptId(2)
                .stepInstanceUuid("bar")
                .stepName("step1")
                .stepInstanceId(123)
                .type(StepType.NOOP)
                .build(),
            "maestro_workflow_runtime_summary",
            Collections.singletonMap("instance_status", "CREATED")));

    Assert.assertTrue(endTask.execute(workflow, testTask, null));

    verify(rollupAggregationHelper, times(1)).calculateRollupBase(any());
    // verify rollupBase set
    Assert.assertNotNull(
        ((WorkflowRuntimeSummary) testTask.getOutputData().get("maestro_workflow_runtime_summary"))
            .getRollupBase());
    Assert.assertEquals(
        1,
        ((WorkflowRuntimeSummary) testTask.getOutputData().get("maestro_workflow_runtime_summary"))
            .getRollupBase()
            .getTotalLeafCount());
    Assert.assertEquals(
        1,
        ((WorkflowRuntimeSummary) testTask.getOutputData().get("maestro_workflow_runtime_summary"))
            .getRollupBase()
            .getOverview()
            .size());

    verify(publisher, times(1)).publish(any(WorkflowInstanceUpdateJobEvent.class));
    WorkflowRuntimeSummary runtimeSummary =
        (WorkflowRuntimeSummary) testTask.getOutputData().get("maestro_workflow_runtime_summary");
    Assert.assertEquals(WorkflowInstance.Status.IN_PROGRESS, runtimeSummary.getInstanceStatus());
    Assert.assertEquals(123456L, runtimeSummary.getStartTime().longValue());
    Assert.assertEquals(1, runtimeSummary.getTimeline().getTimelineEvents().size());
    Assert.assertEquals(
        TimelineLogEvent.builder()
            .timestamp(234567L)
            .message("Workflow instance is dequeued.")
            .level(TimelineEvent.Level.INFO)
            .build(),
        runtimeSummary.getTimeline().getTimelineEvents().get(0));
  }
}
