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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroGateTaskTest extends MaestroEngineBaseTest {

  @Mock private MaestroStepInstanceDao stepInstanceDao;

  private MaestroGateTask gateTask;
  private Workflow workflow;
  private Task joinTask;

  @Before
  public void setup() {
    gateTask = new MaestroGateTask(stepInstanceDao, MAPPER);

    joinTask = new Task();
    joinTask.setInputData(Collections.singletonMap("joinOn", Collections.singletonList("job1")));
    joinTask.setTaskId("test-join-id");
    joinTask.setReferenceTaskName("#job2");

    Task task1 = new Task();
    task1.setStatus(Task.Status.FAILED);
    task1.setTaskId("test-join-id");
    task1.setReferenceTaskName("job1");
    task1.setWorkflowTask(new WorkflowTask());

    workflow = new Workflow();
    workflow.setWorkflowId("testWorkflowId");
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
    WorkflowDef def = new WorkflowDef();
    workflow.setWorkflowDefinition(def);
    Map<String, Object> summary = new HashMap<>();
    summary.put("workflow_id", "testWorkflowId");
    summary.put("workflow_instance_id", 123);
    summary.put("workflow_run_id", 1);
    workflow.setInput(Collections.singletonMap("maestro_workflow_summary", summary));
    workflow.setTasks(Arrays.asList(task1, joinTask));
  }

  @Test
  public void testExecuteDone() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.COMPLETED_WITH_ERROR);
    when(stepInstanceDao.getStepStates(anyString(), anyLong(), anyLong(), anyList()))
        .thenReturn(Collections.singletonMap("job1", state));

    assertTrue(gateTask.execute(workflow, joinTask, null));
    assertEquals(Task.Status.FAILED, joinTask.getStatus());
  }

  @Test
  public void testExecuteIncomplete() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.FATALLY_FAILED);
    when(stepInstanceDao.getStepStates(anyString(), anyLong(), anyLong(), anyList()))
        .thenReturn(Collections.singletonMap("job1", state));

    assertFalse(gateTask.execute(workflow, joinTask, null));
    assertNull(joinTask.getStatus());
  }
}
