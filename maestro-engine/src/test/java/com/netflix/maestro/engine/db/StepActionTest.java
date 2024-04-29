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
package com.netflix.maestro.engine.db;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import org.junit.BeforeClass;
import org.junit.Test;

public class StepActionTest extends MaestroBaseTest {

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRestartRoundTripSerde() throws Exception {
    StepAction expected =
        loadObject("fixtures/db/sample-step-action-restart.json", StepAction.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    StepAction actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), StepAction.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testRestartCreate() {
    StepInstance stepInstance = new StepInstance();
    stepInstance.setWorkflowId("wf1");
    stepInstance.setWorkflowInstanceId(1);
    stepInstance.setWorkflowRunId(2);
    stepInstance.setStepId("job1");
    RunRequest runRequest =
        RunRequest.builder()
            .runParams(singletonMap("foo", ParamDefinition.buildParamDefinition("foo", "bar")))
            .requester(User.create("user1"))
            .stepRunParams(
                singletonMap(
                    "job1", singletonMap("abc", ParamDefinition.buildParamDefinition("abc", 123))))
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .build();
    StepAction action = StepAction.createRestart(stepInstance, runRequest);
    assertEquals(stepInstance.getWorkflowId(), action.getWorkflowId());
    assertEquals(stepInstance.getWorkflowInstanceId(), action.getWorkflowInstanceId());
    assertEquals(stepInstance.getWorkflowRunId(), action.getWorkflowRunId());
    assertEquals(Actions.StepInstanceAction.RESTART, action.getAction());
    assertEquals("user1", action.getUser().getName());
    assertEquals(
        twoItemMap(
            "foo",
            ParamDefinition.buildParamDefinition("foo", "bar"),
            "abc",
            ParamDefinition.buildParamDefinition("abc", 123)),
        action.getRunParams());
  }

  @Test
  public void testInvalidRestartCreate() {
    StepInstance stepInstance = new StepInstance();
    stepInstance.setWorkflowId("wf1");
    stepInstance.setWorkflowInstanceId(1);
    stepInstance.setWorkflowRunId(2);
    stepInstance.setStepId("job1");
    RunRequest runRequest =
        RunRequest.builder()
            .runParams(singletonMap("foo", ParamDefinition.buildParamDefinition("foo", "bar")))
            .requester(User.create("user1"))
            .stepRunParams(
                singletonMap(
                    "job1", singletonMap("abc", ParamDefinition.buildParamDefinition("abc", 123))))
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .build();
    AssertHelper.assertThrows(
        "Invalid step restart policy",
        IllegalArgumentException.class,
        "Invalid step restart policy [RESTART_FROM_BEGINNING] for step [wf1][1][2][job1]",
        () -> StepAction.createRestart(stepInstance, runRequest));
  }

  @Test
  public void testTerminateRoundTripSerde() throws Exception {
    StepAction expected = loadObject("fixtures/db/sample-step-action-stop.json", StepAction.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    StepAction actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), StepAction.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testGetTerminalStatus() {
    StepAction action =
        StepAction.builder()
            .workflowId("sample-dag-test-3")
            .workflowInstanceId(1)
            .stepId("job1")
            .action(Actions.StepInstanceAction.STOP)
            .build();
    assertEquals(StepInstance.Status.STOPPED, action.getTerminalStatus("sample-dag-test-3", 1));
    assertEquals(StepInstance.Status.STOPPED, action.getTerminalStatus("downstream-wf", 2));
    action =
        StepAction.builder()
            .workflowId("sample-dag-test-3")
            .workflowInstanceId(1)
            .stepId("job1")
            .action(Actions.StepInstanceAction.KILL)
            .build();
    assertEquals(
        StepInstance.Status.FATALLY_FAILED, action.getTerminalStatus("sample-dag-test-3", 1));
    assertEquals(StepInstance.Status.STOPPED, action.getTerminalStatus("downstream-wf", 2));
    action =
        StepAction.builder()
            .workflowId("sample-dag-test-3")
            .workflowInstanceId(1)
            .stepId("job1")
            .action(Actions.StepInstanceAction.SKIP)
            .build();
    assertEquals(StepInstance.Status.SKIPPED, action.getTerminalStatus("sample-dag-test-3", 1));
    assertEquals(StepInstance.Status.STOPPED, action.getTerminalStatus("downstream-wf", 2));

    StepAction invalidAction =
        StepAction.builder()
            .workflowId("sample-dag-test-3")
            .workflowInstanceId(1)
            .stepId("job1")
            .action(Actions.StepInstanceAction.PAUSE)
            .build();
    AssertHelper.assertThrows(
        "Cannot call getTerminalStatus for non-termination actions",
        IllegalArgumentException.class,
        "[sample-dag-test-3][1] cannot getTerminalStatus for action",
        () -> invalidAction.getTerminalStatus("sample-dag-test-3", 1));
  }

  @Test
  public void testBypassStepDependenciesRoundTripSerde() throws Exception {
    StepAction expected =
        loadObject(
            "fixtures/db/sample-step-action-bypass-step-dependencies.json", StepAction.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    StepAction actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), StepAction.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }
}
