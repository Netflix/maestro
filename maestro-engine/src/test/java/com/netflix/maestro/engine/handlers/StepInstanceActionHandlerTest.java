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
package com.netflix.maestro.engine.handlers;

import static com.netflix.maestro.models.Actions.StepInstanceAction.STOP;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowInstanceAggregatedInfo;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class StepInstanceActionHandlerTest extends MaestroEngineBaseTest {

  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroStepInstanceActionDao actionDao;
  @Mock private WorkflowInstanceActionHandler actionHandler;
  @Mock private WorkflowInstance instance;

  private StepInstanceActionHandler stepActionHandler;
  private final User user = User.create("tester");

  @Before
  public void before() throws Exception {
    stepActionHandler = new StepInstanceActionHandler(instanceDao, actionDao, actionHandler);
    when(instanceDao.getLatestWorkflowInstanceRun("sample-minimal-wf", 1)).thenReturn(instance);
    when(instanceDao.getWorkflowInstanceRun("sample-minimal-wf", 1, 2L)).thenReturn(instance);
  }

  @Test
  public void testRestartNewRun() {
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    when(instance.getInitiator()).thenReturn(new ManualInitiator());
    when(actionHandler.restartRecursively(any()))
        .thenReturn(RunResponse.builder().status(RunResponse.Status.WORKFLOW_RUN_CREATED).build());
    RunRequest runRequest =
        RunRequest.builder()
            .requester(user)
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .restartConfig(
                RestartConfig.builder().addRestartNode("sample-minimal-wf", 1, "job1").build())
            .build();
    RunResponse response = stepActionHandler.restart(runRequest, true);

    ArgumentCaptor<RunRequest> requestCaptor = ArgumentCaptor.forClass(RunRequest.class);
    Mockito.verify(actionHandler, Mockito.times(1)).restartRecursively(requestCaptor.capture());
    RunRequest request = requestCaptor.getValue();
    assertEquals(runRequest, request);
    assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, response.getStatus());
  }

  @Test
  public void testRestartNewAttempt() {
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(actionHandler.restartRecursively(any()))
        .thenReturn(RunResponse.builder().status(RunResponse.Status.DELEGATED).build());
    RunRequest runRequest =
        RunRequest.builder()
            .requester(user)
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .restartConfig(
                RestartConfig.builder().addRestartNode("sample-minimal-wf", 1, "job1").build())
            .build();
    RunResponse runResponse =
        RunResponse.builder().status(RunResponse.Status.STEP_ATTEMPT_CREATED).build();
    when(actionDao.restartDirectly(any(), any(), anyBoolean())).thenReturn(runResponse);
    RunResponse response = stepActionHandler.restart(runRequest, true);

    ArgumentCaptor<RunRequest> requestCaptor = ArgumentCaptor.forClass(RunRequest.class);
    Mockito.verify(actionHandler, Mockito.times(1)).restartRecursively(requestCaptor.capture());
    Mockito.verify(actionDao, Mockito.times(1)).restartDirectly(any(), eq(runRequest), eq(true));
    RunRequest request = requestCaptor.getValue();

    assertEquals(runRequest, request);
    assertEquals(RunResponse.Status.STEP_ATTEMPT_CREATED, response.getStatus());
    assertEquals(runResponse, response);
  }

  @Test
  public void testRestartFromInlineRootWithinForeach() {
    when(instanceDao.getWorkflowInstance(
            "sample-minimal-wf", 1, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(instance);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);

    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId("root-wf");
    parent.setInstanceId(123);
    parent.setRunId(1);
    parent.setStepId("foreach-step");
    parent.setStepAttemptId(1);
    initiator.setAncestors(Collections.singletonList(parent));
    when(instance.getInitiator()).thenReturn(initiator);

    WorkflowInstanceAggregatedInfo aggregatedInfo = mock(WorkflowInstanceAggregatedInfo.class);
    when(instance.getAggregatedInfo()).thenReturn(aggregatedInfo);
    StepAggregatedView aggregatedView = mock(StepAggregatedView.class);
    when(aggregatedInfo.getStepAggregatedViews()).thenReturn(singletonMap("job1", aggregatedView));
    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.FATALLY_FAILED);

    when(actionHandler.restartRecursively(any()))
        .thenReturn(RunResponse.builder().status(RunResponse.Status.WORKFLOW_RUN_CREATED).build());

    RunRequest runRequest =
        RunRequest.builder()
            .requester(user)
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode("sample-minimal-wf", 1, "job1")
                    .restartPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
                    .stepRestartParams(
                        Collections.singletonMap(
                            "job1",
                            Collections.singletonMap(
                                "param1", ParamDefinition.buildParamDefinition("param1", "foo"))))
                    .build())
            .build();

    RunResponse response = stepActionHandler.restart(runRequest, true);

    ArgumentCaptor<RunRequest> requestCaptor = ArgumentCaptor.forClass(RunRequest.class);
    Mockito.verify(actionHandler, Mockito.times(1)).restartRecursively(requestCaptor.capture());
    RunRequest request = requestCaptor.getValue();

    assertEquals(runRequest, request);
    assertEquals(
        Collections.singletonList(new RestartConfig.RestartNode("root-wf", 123, null)),
        request.getRestartConfig().getRestartPath());
    assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, response.getStatus());
  }

  @Test
  public void testRestartFromInlineRootWithinNonForeach() {
    when(instanceDao.getWorkflowInstance(
            "sample-minimal-wf", 1, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(instance);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    when(instance.getInitiator()).thenReturn(new ManualInitiator());

    WorkflowInstanceAggregatedInfo aggregatedInfo = mock(WorkflowInstanceAggregatedInfo.class);
    when(instance.getAggregatedInfo()).thenReturn(aggregatedInfo);
    StepAggregatedView aggregatedView = mock(StepAggregatedView.class);
    when(aggregatedInfo.getStepAggregatedViews()).thenReturn(singletonMap("job1", aggregatedView));
    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.FATALLY_FAILED);

    when(actionHandler.restartRecursively(any()))
        .thenReturn(RunResponse.builder().status(RunResponse.Status.WORKFLOW_RUN_CREATED).build());

    RunRequest runRequest =
        RunRequest.builder()
            .requester(user)
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode("sample-minimal-wf", 1, "job1")
                    .restartPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
                    .stepRestartParams(
                        Collections.singletonMap(
                            "job1",
                            Collections.singletonMap(
                                "param1", ParamDefinition.buildParamDefinition("param1", "foo"))))
                    .build())
            .build();

    RunResponse response = stepActionHandler.restart(runRequest, true);

    ArgumentCaptor<RunRequest> requestCaptor = ArgumentCaptor.forClass(RunRequest.class);
    Mockito.verify(actionHandler, Mockito.times(1)).restartRecursively(requestCaptor.capture());
    RunRequest request = requestCaptor.getValue();

    assertEquals(runRequest, request);
    assertEquals(
        Collections.singletonList(new RestartConfig.RestartNode("sample-minimal-wf", 1, null)),
        request.getRestartConfig().getRestartPath());
    assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, response.getStatus());
  }

  @Test
  public void testInvalidRestartFromInlineRoot() {
    when(instanceDao.getWorkflowInstance(
            "sample-minimal-wf", 1, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(instance);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(instance.getInitiator()).thenReturn(new ForeachInitiator());
    RunRequest runRequest =
        RunRequest.builder()
            .requester(user)
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode("sample-minimal-wf2", 1, "job1")
                    .addRestartNode("sample-minimal-wf", 1, "job1")
                    .build())
            .build();
    AssertHelper.assertThrows(
        "Cannot restart from inline root for non-terminal root",
        IllegalArgumentException.class,
        "instance [null] is in non-terminal state [IN_PROGRESS]",
        () -> stepActionHandler.restart(runRequest, true));

    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    WorkflowInstanceAggregatedInfo aggregatedInfo = mock(WorkflowInstanceAggregatedInfo.class);
    when(instance.getAggregatedInfo()).thenReturn(aggregatedInfo);
    StepAggregatedView aggregatedView = mock(StepAggregatedView.class);
    when(aggregatedInfo.getStepAggregatedViews()).thenReturn(singletonMap("job1", aggregatedView));
    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.RUNNING);
    AssertHelper.assertThrows(
        "Cannot restart from inline root for non-terminal step",
        IllegalArgumentException.class,
        "step null[job1] is in non-terminal state [RUNNING]",
        () -> stepActionHandler.restart(runRequest, true));

    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.FATALLY_FAILED);
    AssertHelper.assertThrows(
        "Cannot restart from inline root for invalid restart path",
        IllegalArgumentException.class,
        "restart-path size is not 1",
        () -> stepActionHandler.restart(runRequest, true));
  }

  @Test
  public void testTerminate() {
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    stepActionHandler.terminate("sample-minimal-wf", 1, "job1", user, STOP);
    verify(actionDao, times(1)).terminate(instance, "job1", user, STOP);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    AssertHelper.assertThrows(
        "Cannot manually terminate the step",
        MaestroInvalidStatusException.class,
        "Cannot manually STOP the step [job1] as the workflow instance",
        () -> stepActionHandler.terminate("sample-minimal-wf", 1, "job1", user, STOP));
  }

  @Test
  public void testBypassDependencies() {
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    stepActionHandler.bypassStepDependencies("sample-minimal-wf", 1, "job1", user);
    verify(actionDao, times(1)).bypassStepDependencies(instance, "job1", user);
  }

  @Test
  public void testSkipRunningStepInRunningInstance() {
    when(instanceDao.getWorkflowInstance(
            "sample-minimal-wf", 1, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(instance);
    WorkflowInstanceAggregatedInfo aggregatedInfo = mock(WorkflowInstanceAggregatedInfo.class);
    when(instance.getAggregatedInfo()).thenReturn(aggregatedInfo);
    StepAggregatedView aggregatedView = mock(StepAggregatedView.class);
    when(aggregatedInfo.getStepAggregatedViews()).thenReturn(singletonMap("job1", aggregatedView));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.RUNNING);
    stepActionHandler.skip("sample-minimal-wf", 1, "job1", user, null, true);
    verify(actionDao, times(1)).terminate(instance, "job1", user, Actions.StepInstanceAction.SKIP);
  }

  @Test
  public void testSkipFailedStepInRunningInstance() {
    when(instanceDao.getWorkflowInstance(
            "sample-minimal-wf", 1, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(instance);
    WorkflowInstanceAggregatedInfo aggregatedInfo = mock(WorkflowInstanceAggregatedInfo.class);
    when(instance.getAggregatedInfo()).thenReturn(aggregatedInfo);
    StepAggregatedView aggregatedView = mock(StepAggregatedView.class);
    when(aggregatedInfo.getStepAggregatedViews()).thenReturn(singletonMap("job1", aggregatedView));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.FATALLY_FAILED);
    RunRequest runRequest =
        RunRequest.builder()
            .requester(user)
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode("sample-minimal-wf", 1, "job1")
                    .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
                    .skipSteps(Collections.singleton("job1"))
                    .build())
            .build();
    when(actionHandler.restartRecursively(runRequest))
        .thenReturn(RunResponse.builder().status(RunResponse.Status.DELEGATED).build());
    when(actionDao.restartDirectly(any(), eq(runRequest), eq(true)))
        .thenReturn(RunResponse.builder().status(RunResponse.Status.STEP_ATTEMPT_CREATED).build());
    stepActionHandler.skip("sample-minimal-wf", 1, "job1", user, runRequest, true);
    verify(actionDao, times(0)).terminate(instance, "job1", user, Actions.StepInstanceAction.SKIP);
    verify(actionHandler, times(1)).restartRecursively(runRequest);
    verify(actionDao, times(1)).restartDirectly(any(), eq(runRequest), eq(true));
  }

  @Test
  public void testSkipShouldWakeupStepInRunningInstance() {
    when(instanceDao.getWorkflowInstance(
            "sample-minimal-wf", 1, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(instance);
    WorkflowInstanceAggregatedInfo aggregatedInfo = mock(WorkflowInstanceAggregatedInfo.class);
    when(instance.getAggregatedInfo()).thenReturn(aggregatedInfo);
    StepAggregatedView aggregatedView = mock(StepAggregatedView.class);
    when(aggregatedInfo.getStepAggregatedViews()).thenReturn(singletonMap("job1", aggregatedView));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.PLATFORM_FAILED);
    stepActionHandler.skip("sample-minimal-wf", 1, "job1", user, null, true);
    verify(actionDao, times(1)).terminate(instance, "job1", user, Actions.StepInstanceAction.SKIP);
  }

  @Test
  public void testSkipStoppedStepInStoppedInstance() {
    when(instanceDao.getWorkflowInstance(
            "sample-minimal-wf", 1, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(instance);
    WorkflowInstanceAggregatedInfo aggregatedInfo = mock(WorkflowInstanceAggregatedInfo.class);
    when(instance.getAggregatedInfo()).thenReturn(aggregatedInfo);
    StepAggregatedView aggregatedView = mock(StepAggregatedView.class);
    when(aggregatedInfo.getStepAggregatedViews()).thenReturn(singletonMap("job1", aggregatedView));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.STOPPED);
    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.STOPPED);

    RunRequest runRequest =
        RunRequest.builder()
            .requester(user)
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode("sample-minimal-wf", 1, "job1")
                    .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
                    .skipSteps(Collections.singleton("job1"))
                    .build())
            .build();
    when(actionHandler.restartRecursively(runRequest))
        .thenReturn(RunResponse.builder().status(RunResponse.Status.WORKFLOW_RUN_CREATED).build());
    stepActionHandler.skip("sample-minimal-wf", 1, "job1", user, runRequest, true);
    verify(actionDao, times(0)).terminate(instance, "job1", user, Actions.StepInstanceAction.SKIP);
    verify(actionHandler, times(1)).restartRecursively(runRequest);
    verify(actionDao, times(0)).restartDirectly(any(), eq(runRequest), eq(true));
  }

  @Test
  public void testInvalidSkip() {
    when(instanceDao.getWorkflowInstance(
            "sample-minimal-wf", 1, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(instance);
    WorkflowInstanceAggregatedInfo aggregatedInfo = mock(WorkflowInstanceAggregatedInfo.class);
    when(instance.getAggregatedInfo()).thenReturn(aggregatedInfo);
    StepAggregatedView aggregatedView = mock(StepAggregatedView.class);
    when(aggregatedInfo.getStepAggregatedViews()).thenReturn(singletonMap("job1", aggregatedView));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.RUNNING);
    AssertHelper.assertThrows(
        "Cannot find status in aggregated step views",
        NullPointerException.class,
        "Invalid: cannot find the step view of workflow step ",
        () -> stepActionHandler.skip("sample-minimal-wf", 1, "job2", user, null, true));

    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.NOT_CREATED);
    AssertHelper.assertThrows(
        "Cannot skip not-created step",
        MaestroBadRequestException.class,
        "Cannot skip step [sample-minimal-wf][1][job1] before it is created. Please try it again.",
        () -> stepActionHandler.skip("sample-minimal-wf", 1, "job1", user, null, true));

    when(aggregatedView.getStatus()).thenReturn(StepInstance.Status.CREATED);
    AssertHelper.assertThrows(
        "Cannot skip not-created step",
        MaestroBadRequestException.class,
        "Cannot skip step [sample-minimal-wf][1][job1] because it is unsupported by the step action map",
        () -> stepActionHandler.skip("sample-minimal-wf", 1, "job1", user, null, true));
  }
}
