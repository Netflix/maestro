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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.Constants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class WorkflowTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    Workflow wf =
        loadObject(
                "fixtures/workflows/definition/sample-active-wf-with-props.json",
                WorkflowDefinition.class)
            .getWorkflow();
    assertEquals(wf, MAPPER.readValue(MAPPER.writeValueAsString(wf), Workflow.class));
  }

  @Test
  public void testGetDag() throws Exception {
    Workflow wf =
        loadObject(
                "fixtures/workflows/definition/sample-active-wf-with-props.json",
                WorkflowDefinition.class)
            .getWorkflow();
    assertEquals(
        threeItemMap(
            "job.1", wf.getSteps().get(0).getTransition(),
            "job.2", wf.getSteps().get(1).getTransition(),
            "job.3", wf.getSteps().get(2).getTransition()),
        wf.getDag());
  }

  @Test
  public void testGetAllStepIds() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    Workflow workflow =
        Workflow.builder().steps(Collections.nCopies(Constants.STEP_LIST_SIZE_LIMIT, step)).build();
    assertEquals(Constants.STEP_LIST_SIZE_LIMIT, workflow.getAllStepIds().size());
    assertEquals(
        Collections.nCopies(Constants.STEP_LIST_SIZE_LIMIT, "foo"), workflow.getAllStepIds());
  }

  @Test
  public void testGetAllStepIdsInNestedForeach() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    ForeachStep foreachStep = new ForeachStep();
    foreachStep.setId("foreach-step");
    foreachStep.setSteps(Collections.nCopies(Constants.STEP_LIST_SIZE_LIMIT - 1, step));

    Workflow workflow = Workflow.builder().steps(Collections.singletonList(foreachStep)).build();
    assertEquals(Constants.STEP_LIST_SIZE_LIMIT, workflow.getAllStepIds().size());
    List<String> expected = new ArrayList<>();
    expected.add("foreach-step");
    expected.addAll(Collections.nCopies(Constants.STEP_LIST_SIZE_LIMIT - 1, "foo"));
    assertEquals(expected, workflow.getAllStepIds());
  }

  @Test
  public void testGetWorkflowNameOrDefault() {
    Workflow wf = Workflow.builder().id("test-wf").build();
    assertEquals("test-wf", wf.getWorkflowNameOrDefault());
    wf = Workflow.builder().id("test-wf").name("test-name").build();
    assertEquals("test-name", wf.getWorkflowNameOrDefault());
  }
}
