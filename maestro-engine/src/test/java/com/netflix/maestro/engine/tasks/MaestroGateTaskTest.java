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

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowDef;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.models.TaskDef;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroGateTaskTest extends MaestroEngineBaseTest {

  @Mock private MaestroStepInstanceDao stepInstanceDao;

  private MaestroGateTask gateTask;
  private Flow flow;
  private Task joinTask;

  @Before
  public void setup() {
    gateTask = new MaestroGateTask(stepInstanceDao, MAPPER);

    joinTask = new Task();
    TaskDef taskDef =
        new TaskDef("#job2", StepType.JOIN.name(), null, Collections.singletonList("job1"));
    joinTask.setTaskDef(taskDef);
    joinTask.setTaskId("test-join-id");

    Task task1 = new Task();
    task1.setStatus(Task.Status.FAILED);
    task1.setTaskId("test-join-id");
    TaskDef taskDef1 = new TaskDef("job1", Constants.MAESTRO_TASK_NAME, null, null);
    task1.setTaskDef(taskDef1);

    flow = new Flow(1, "testWorkflowId", 1, 12345, "ref");
    flow.setStatus(Flow.Status.RUNNING);
    FlowDef def = new FlowDef();
    flow.setFlowDef(def);
    Map<String, Object> summary = new HashMap<>();
    summary.put("workflow_id", "testWorkflowId");
    summary.put("workflow_instance_id", 123);
    summary.put("workflow_run_id", 1);
    flow.setInput(Collections.singletonMap("maestro_workflow_summary", summary));
    flow.addFinishedTask(task1);
    flow.updateRunningTask(joinTask);
  }

  @Test
  public void testExecuteDone() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.COMPLETED_WITH_ERROR);
    when(stepInstanceDao.getStepStates(anyString(), anyLong(), anyLong(), anyList()))
        .thenReturn(Collections.singletonMap("job1", state));

    assertTrue(gateTask.execute(flow, joinTask));
    assertEquals(Task.Status.FAILED, joinTask.getStatus());
  }

  @Test
  public void testExecuteIncomplete() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.FATALLY_FAILED);
    when(stepInstanceDao.getStepStates(anyString(), anyLong(), anyLong(), anyList()))
        .thenReturn(Collections.singletonMap("job1", state));

    assertFalse(gateTask.execute(flow, joinTask));
    assertNull(joinTask.getStatus());
  }
}
