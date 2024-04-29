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

import static org.mockito.Mockito.mock;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.api.InstanceRunStatus;
import com.netflix.maestro.models.api.StepInstanceActionResponse;
import com.netflix.maestro.models.api.StepInstanceRestartResponse;
import com.netflix.maestro.models.api.WorkflowInstanceRestartResponse;
import com.netflix.maestro.models.api.WorkflowStartResponse;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class RunResponseTest extends MaestroEngineBaseTest {
  @Mock private WorkflowInstance instance;
  @Mock private StepInstance stepInstance;

  @Before
  public void setUp() {
    Mockito.when(instance.getWorkflowId()).thenReturn("test-workflow");
    Mockito.when(instance.getWorkflowVersionId()).thenReturn(12L);
    Mockito.when(instance.getWorkflowInstanceId()).thenReturn(1L);
    Mockito.when(instance.getWorkflowRunId()).thenReturn(2L);
    Mockito.when(instance.getInitiator()).thenReturn(mock(Initiator.class));
    Mockito.when(instance.getStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
  }

  @Test
  public void testBuildFromState() {
    RunResponse res = RunResponse.from(instance, 1);
    Assert.assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, res.getStatus());
    res = RunResponse.from(instance, 0);
    Assert.assertEquals(RunResponse.Status.DUPLICATED, res.getStatus());
    res = RunResponse.from(instance, -1);
    Assert.assertEquals(RunResponse.Status.STOPPED, res.getStatus());
    AssertHelper.assertThrows(
        "Invalid status code",
        MaestroInternalError.class,
        "Invalid status code value: 2",
        () -> RunResponse.from(instance, 2));
  }

  @Test
  public void testBuildFromStepId() {
    RunResponse res = RunResponse.from(instance, "foo");
    Assert.assertEquals(RunResponse.Status.DELEGATED, res.getStatus());
    res = RunResponse.from(instance, null);
    Assert.assertEquals(RunResponse.Status.NON_TERMINAL_ERROR, res.getStatus());
  }

  @Test
  public void testBuildFromEvent() {
    RunResponse res = RunResponse.from(stepInstance, TimelineLogEvent.info("bar"));
    Assert.assertEquals(RunResponse.Status.STEP_ATTEMPT_CREATED, res.getStatus());
  }

  @Test
  public void testToWorkflowStartResponse() {
    RunResponse res = RunResponse.from(stepInstance, TimelineLogEvent.info("bar"));
    WorkflowStartResponse response = res.toWorkflowStartResponse();
    Assert.assertEquals(InstanceRunStatus.CREATED, response.getStatus());
    res = RunResponse.from(instance, "foo");
    response = res.toWorkflowStartResponse();
    Assert.assertEquals(InstanceRunStatus.INTERNAL_ERROR, response.getStatus());
    res = RunResponse.from(instance, 0);
    response = res.toWorkflowStartResponse();
    Assert.assertEquals(InstanceRunStatus.DUPLICATED, response.getStatus());
    res = RunResponse.from(instance, -1);
    response = res.toWorkflowStartResponse();
    Assert.assertEquals(InstanceRunStatus.STOPPED, response.getStatus());
    res = RunResponse.from(instance, 1);
    response = res.toWorkflowStartResponse();
    Assert.assertEquals(InstanceRunStatus.CREATED, response.getStatus());
  }

  @Test
  public void testToWorkflowRestartResponse() {
    RunResponse res = RunResponse.from(stepInstance, TimelineLogEvent.info("bar"));
    WorkflowInstanceRestartResponse response = res.toWorkflowRestartResponse();
    Assert.assertEquals(InstanceRunStatus.CREATED, response.getStatus());
    res = RunResponse.from(instance, "foo");
    response = res.toWorkflowRestartResponse();
    Assert.assertEquals(InstanceRunStatus.INTERNAL_ERROR, response.getStatus());
    res = RunResponse.from(instance, 0);
    response = res.toWorkflowRestartResponse();
    Assert.assertEquals(InstanceRunStatus.DUPLICATED, response.getStatus());
    res = RunResponse.from(instance, -1);
    response = res.toWorkflowRestartResponse();
    Assert.assertEquals(InstanceRunStatus.STOPPED, response.getStatus());
    res = RunResponse.from(instance, 1);
    response = res.toWorkflowRestartResponse();
    Assert.assertEquals(InstanceRunStatus.CREATED, response.getStatus());
  }

  @Test
  public void testToStepRestartResponse() {
    RunResponse res = RunResponse.from(stepInstance, TimelineLogEvent.info("bar"));
    StepInstanceRestartResponse response = res.toStepRestartResponse();
    Assert.assertEquals(InstanceRunStatus.CREATED, response.getStatus());
    res = RunResponse.from(instance, "foo");
    response = res.toStepRestartResponse();
    Assert.assertEquals(InstanceRunStatus.INTERNAL_ERROR, response.getStatus());
    res = RunResponse.from(instance, 0);
    response = res.toStepRestartResponse();
    Assert.assertEquals(InstanceRunStatus.DUPLICATED, response.getStatus());
    res = RunResponse.from(instance, -1);
    response = res.toStepRestartResponse();
    Assert.assertEquals(InstanceRunStatus.STOPPED, response.getStatus());
    res = RunResponse.from(instance, 1);
    response = res.toStepRestartResponse();
    Assert.assertEquals(InstanceRunStatus.CREATED, response.getStatus());
    Assert.assertEquals(12L, response.getWorkflowVersionId());
  }

  @Test
  public void testToStepInstanceActionResponse() {
    Mockito.when(stepInstance.getStepId()).thenReturn("test-step");
    Mockito.when(stepInstance.getWorkflowId()).thenReturn("test-workflow");
    Mockito.when(stepInstance.getWorkflowInstanceId()).thenReturn(123L);
    Mockito.when(stepInstance.getWorkflowRunId()).thenReturn(2L);
    RunResponse res = RunResponse.from(stepInstance, TimelineLogEvent.info("bar"));
    StepInstanceActionResponse response = res.toStepInstanceActionResponse();
    Assert.assertEquals("test-workflow", response.getWorkflowId());
    Assert.assertEquals(123, response.getWorkflowInstanceId());
    Assert.assertEquals(2, response.getWorkflowRunId());
    Assert.assertEquals("test-step", response.getStepId());
    Assert.assertEquals(1, response.getStepAttemptId().intValue());
    Assert.assertEquals("bar", response.getTimelineEvent().getMessage());
  }
}
