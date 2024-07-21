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
package com.netflix.maestro.engine.eval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.TimeInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class InstanceWrapperTest extends MaestroBaseTest {

  @Test
  public void testFromSummary() throws Exception {
    WorkflowSummary workflowSummary =
        loadObject("fixtures/parameters/sample-wf-summary-params.json", WorkflowSummary.class);
    StepRuntimeSummary stepRuntimeSummary =
        loadObject(
            "fixtures/execution/sample-step-runtime-summary-2.json", StepRuntimeSummary.class);
    InstanceWrapper instanceWrapper = InstanceWrapper.from(workflowSummary, stepRuntimeSummary);
    assertEquals(workflowSummary.getInitiator(), instanceWrapper.getInitiator());
    assertFalse(instanceWrapper.isWorkflowParam());
    assertNull(instanceWrapper.getInitiatorTimeZone());
    assertEquals(workflowSummary.getRunPolicy().name(), instanceWrapper.getRunPolicy());
    assertEquals(
        workflowSummary.getRunProperties().getOwner().getName(),
        instanceWrapper.getWorkflowOwner());
    assertNull(instanceWrapper.getFirstTimeTriggerTimeZone());
    assertEquals(
        stepRuntimeSummary.getStepId(), instanceWrapper.getStepInstanceAttributes().getStepId());
    assertEquals(
        stepRuntimeSummary.getStepInstanceId(),
        instanceWrapper.getStepInstanceAttributes().getStepInstanceId());
    assertEquals(
        stepRuntimeSummary.getStepInstanceUuid(),
        instanceWrapper.getStepInstanceAttributes().getStepInstanceUuid());
    assertEquals(
        stepRuntimeSummary.getType(), instanceWrapper.getStepInstanceAttributes().getType());
    assertEquals(
        stepRuntimeSummary.getSubType(), instanceWrapper.getStepInstanceAttributes().getSubType());
  }

  @Test
  public void testFromInstanceAndRequest() throws Exception {
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    RunRequest runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .requester(User.create("tester1"))
            .build();
    InstanceWrapper instanceWrapper = InstanceWrapper.from(instance, runRequest);
    assertEquals(runRequest.getInitiator(), instanceWrapper.getInitiator());
    assertTrue(instanceWrapper.isWorkflowParam());
    assertNull(instanceWrapper.getInitiatorTimeZone());
    assertEquals(runRequest.getCurrentPolicy().name(), instanceWrapper.getRunPolicy());
    assertEquals(
        instance.getRunProperties().getOwner().getName(), instanceWrapper.getWorkflowOwner());
    assertNull(instanceWrapper.getFirstTimeTriggerTimeZone());
  }

  @Test
  public void testGetInitiatorTimeZone() throws Exception {
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    TimeInitiator initiator = new TimeInitiator();
    initiator.setTimezone("Asia/Tokyo");
    ;
    RunRequest runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .initiator(initiator)
            .build();
    InstanceWrapper instanceWrapper = InstanceWrapper.from(instance, runRequest);
    assertEquals(runRequest.getInitiator(), instanceWrapper.getInitiator());
    assertTrue(instanceWrapper.isWorkflowParam());
    assertEquals("Asia/Tokyo", instanceWrapper.getInitiatorTimeZone());
    assertEquals(runRequest.getCurrentPolicy().name(), instanceWrapper.getRunPolicy());
    assertEquals(
        instance.getRunProperties().getOwner().getName(), instanceWrapper.getWorkflowOwner());
    assertNull(instanceWrapper.getFirstTimeTriggerTimeZone());
  }

  @Test
  public void testGetWorkflowOwner() throws Exception {
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    RunRequest runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .requester(User.create("tester"))
            .runParams(
                Collections.singletonMap(
                    "owner", ParamDefinition.buildParamDefinition("owner", "tester2")))
            .build();

    InstanceWrapper instanceWrapper = InstanceWrapper.from(instance, runRequest);
    AssertHelper.assertThrows(
        "Should not have OWNER set for non system initiated workflow",
        MaestroBadRequestException.class,
        "Should not have OWNER set for non system initiated workflow",
        instanceWrapper::getWorkflowOwner);

    runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .initiator(UpstreamInitiator.withType(Initiator.Type.FOREACH))
            .runParams(
                Collections.singletonMap(
                    "owner", StringParamDefinition.builder().expression("tester2").build()))
            .build();
    instanceWrapper = InstanceWrapper.from(instance, runRequest);
    AssertHelper.assertThrows(
        "System initiator run should set owner as literal instead of using expression",
        IllegalArgumentException.class,
        "System initiator run should set owner as literal instead of using expression",
        instanceWrapper::getWorkflowOwner);

    runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .initiator(UpstreamInitiator.withType(Initiator.Type.FOREACH))
            .runParams(
                Collections.singletonMap(
                    "owner", ParamDefinition.buildParamDefinition("owner", "tester2")))
            .build();
    instanceWrapper = InstanceWrapper.from(instance, runRequest);
    assertEquals("tester2", instanceWrapper.getWorkflowOwner());
  }

  @Test
  public void testGetFirstTimeTriggerTimeZone() throws Exception {
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    CronTimeTrigger cronTrigger1 = new CronTimeTrigger();
    cronTrigger1.setTimezone("US/Pacific");
    CronTimeTrigger cronTrigger2 = new CronTimeTrigger();
    cronTrigger2.setTimezone("UTC");
    instance.setRuntimeWorkflow(
        instance.getRuntimeWorkflow().toBuilder()
            .timeTriggers(Arrays.asList(cronTrigger1, cronTrigger2))
            .build());
    RunRequest runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .requester(User.create("tester"))
            .build();

    InstanceWrapper instanceWrapper = InstanceWrapper.from(instance, runRequest);
    assertEquals(runRequest.getInitiator(), instanceWrapper.getInitiator());
    assertTrue(instanceWrapper.isWorkflowParam());
    assertNull(instanceWrapper.getInitiatorTimeZone());
    assertEquals(runRequest.getCurrentPolicy().name(), instanceWrapper.getRunPolicy());
    assertEquals(
        instance.getRunProperties().getOwner().getName(), instanceWrapper.getWorkflowOwner());
    assertEquals("US/Pacific", instanceWrapper.getFirstTimeTriggerTimeZone());
  }
}
