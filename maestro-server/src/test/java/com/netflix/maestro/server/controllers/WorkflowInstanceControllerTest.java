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
package com.netflix.maestro.server.controllers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class WorkflowInstanceControllerTest extends MaestroBaseTest {
  @Mock private MaestroWorkflowInstanceDao mockInstanceDao;

  private WorkflowInstanceController workflowInstanceController;

  @Before
  public void before() {
    this.mockInstanceDao = mock(MaestroWorkflowInstanceDao.class);
    this.workflowInstanceController = new WorkflowInstanceController(this.mockInstanceDao);
  }

  @Test
  public void testGetInlineWorkflowIdForStepWithinForeach() throws Exception {
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-nested-inline-foreach.json",
            WorkflowInstance.class);
    instance.setWorkflowId("nested-foreach-wf1");
    instance.setWorkflowInstanceId(20);
    when(mockInstanceDao.getWorkflowInstanceRun("nested-foreach-wf1", 10, 0)).thenReturn(instance);
    ForeachInitiator initiator = (ForeachInitiator) instance.getInitiator();
    initiator.getAncestors().add(new UpstreamInitiator.Info());
    initiator.getParent().setStepId("test-step1");
    initiator.getParent().setInstanceId(7);

    assertEquals(
        "maestro_foreach_F3_1K_a9533975fe5e3b9ce03338237ddfa6a1",
        workflowInstanceController.getInlineWorkflowIdForStepWithinForeach(
            initiator.getAncestors()));

    initiator.getParent().setStepId("not-existing");
    AssertHelper.assertThrows(
        "Inline workflow path is empty and invalid",
        IllegalArgumentException.class,
        "Inline workflow step path [[]] is invalid for the input path ",
        () ->
            workflowInstanceController.getInlineWorkflowIdForStepWithinForeach(
                initiator.getAncestors()));

    initiator.getParent().setStepId("test-step2");
    AssertHelper.assertThrows(
        "Inline workflow path length mismatch with the initiator",
        IllegalArgumentException.class,
        "Inline workflow step path [[root-step, test-step2]] is invalid for the input",
        () ->
            workflowInstanceController.getInlineWorkflowIdForStepWithinForeach(
                initiator.getAncestors()));
  }

  @Test
  public void getForeachInitiatorForInlineWorkflowInstance() throws Exception {
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-nested-inline-foreach.json",
            WorkflowInstance.class);
    instance.setWorkflowInstanceId(20);
    when(mockInstanceDao.getWorkflowInstanceRun("maestro_foreach_F3_1K_inline_instance", 7, 0))
        .thenReturn(instance);

    List<UpstreamInitiator.Info> path =
        workflowInstanceController.getForeachInitiatorForInlineWorkflowInstance(
            "maestro_foreach_F3_1K_inline_instance", 7);
    assertEquals(3, path.size());
    assertEquals("nested-foreach-wf1", path.get(0).getWorkflowId());
    assertEquals(10, path.get(0).getInstanceId());
    assertEquals(
        Arrays.asList(10L, 3L, 7L),
        path.stream().map(UpstreamInitiator.Info::getInstanceId).collect(Collectors.toList()));
  }
}
