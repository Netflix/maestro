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

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.SubworkflowInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class StepHelperTest extends MaestroEngineBaseTest {

  @Mock private StepRuntimeState state;

  private StepRuntimeSummary summary;

  @Before
  public void setup() {
    summary = StepRuntimeSummary.builder().runtimeState(state).build();
  }

  @Test
  public void testRetrieveStepRuntimeStateObject() {
    Assert.assertEquals(
        state,
        StepHelper.retrieveStepRuntimeState(
            singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary), MAPPER));
  }

  @Test
  public void testRetrieveStepRuntimeStateJson() {
    StepRuntimeState expected = new StepRuntimeState();
    expected.setStatus(StepInstance.Status.RUNNING);
    Assert.assertEquals(
        expected,
        StepHelper.retrieveStepRuntimeState(
            singletonMap(
                Constants.STEP_RUNTIME_SUMMARY_FIELD,
                singletonMap("runtime_state", singletonMap("status", "RUNNING"))),
            MAPPER));
  }

  @Test
  public void testRetrieveStepRuntimeStateNotExists() {
    StepRuntimeState expected = new StepRuntimeState();
    Assert.assertEquals(
        expected,
        StepHelper.retrieveStepRuntimeState(
            singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, Collections.emptyMap()), MAPPER));
  }

  @Test
  public void testErrorForTooDeepWorkflow() {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId("test-workflow");
    summary.setWorkflowInstanceId(123);
    summary.setWorkflowRunId(2);
    SubworkflowInitiator initiator = new SubworkflowInitiator();
    UpstreamInitiator.Info info = new UpstreamInitiator.Info();
    info.setWorkflowId("foo");
    initiator.setAncestors(Arrays.asList(info, info, info, info, info, info, info, info, info));
    summary.setInitiator(initiator);

    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .type(StepType.SUBWORKFLOW)
            .stepId("bar")
            .stepAttemptId(3)
            .build();

    AssertHelper.assertThrows(
        "Workflow depth is not less than the depth limit",
        IllegalArgumentException.class,
        "is not less than the depth limit: 10",
        () ->
            StepHelper.createInternalWorkflowRunRequest(
                summary, runtimeSummary, null, null, null, null));
  }

  @Test
  public void testCreateInternalWorkflowRunRequest() {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId("test-workflow");
    summary.setWorkflowInstanceId(123);
    summary.setWorkflowRunId(2);
    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info info = new UpstreamInitiator.Info();
    info.setWorkflowId("foo");
    initiator.setAncestors(Collections.singletonList(info));
    summary.setInitiator(initiator);
    summary.setRunPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    RestartConfig restartConfig =
        RestartConfig.builder()
            .addRestartNode("foo", 1, "bar")
            .addRestartNode("foo", 2, "bar")
            .restartPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .downstreamPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .build();
    summary.setRestartConfig(restartConfig);

    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .type(StepType.SUBWORKFLOW)
            .stepId("bar")
            .stepAttemptId(3)
            .build();

    RunRequest request =
        StepHelper.createInternalWorkflowRunRequest(
            summary, runtimeSummary, null, null, null, true);
    Assert.assertEquals(restartConfig, request.getRestartConfig());
    Assert.assertFalse(request.getInitiator().getParent().isAsync());

    request.getRestartConfig().getRestartPath().clear();
    Assert.assertEquals(2, restartConfig.getRestartPath().size());
    request.clearRestartFor(RunPolicy.RESTART_FROM_SPECIFIC);
    Assert.assertEquals(2, restartConfig.getRestartPath().size());
    Assert.assertEquals(RunPolicy.RESTART_FROM_BEGINNING, restartConfig.getRestartPolicy());
  }

  @Test
  public void testCreateInternalWorkflowRunRequestWithNullSyncMode() {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId("test-workflow");
    summary.setWorkflowInstanceId(123);
    summary.setWorkflowRunId(2);
    summary.setRunPolicy(RunPolicy.START_FRESH_NEW_RUN);
    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info info = new UpstreamInitiator.Info();
    info.setWorkflowId("foo");
    initiator.setAncestors(Collections.singletonList(info));
    summary.setInitiator(initiator);

    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .type(StepType.SUBWORKFLOW)
            .stepId("bar")
            .stepAttemptId(3)
            .build();

    RunRequest request =
        StepHelper.createInternalWorkflowRunRequest(
            summary, runtimeSummary, null, null, null, null);
    Assert.assertFalse(
        "isAsync should be false when sync is null", request.getInitiator().getParent().isAsync());
  }
}
