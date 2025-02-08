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
package com.netflix.maestro.engine.dao;

import static com.netflix.maestro.models.Actions.StepInstanceAction.KILL;
import static com.netflix.maestro.models.Actions.StepInstanceAction.RESTART;
import static com.netflix.maestro.models.Actions.StepInstanceAction.SKIP;
import static com.netflix.maestro.models.Actions.StepInstanceAction.STOP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.engine.db.StepAction;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.StepInstanceWakeUpEvent;
import com.netflix.maestro.engine.properties.StepActionProperties;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroTimeoutException;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.api.StepInstanceActionResponse;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.definition.FailureMode;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.SubworkflowInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.ForeachAction;
import com.netflix.maestro.models.instance.ForeachStepOverview;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class MaestroStepInstanceActionDaoTest extends MaestroDaoBaseTest {

  private MaestroStepInstanceActionDao actionDao;
  private MaestroStepInstanceDao stepInstanceDao;
  private WorkflowInstance instance;
  private StepInstance stepInstance;
  @Mock private MaestroJobEventPublisher publisher;

  private final User user = User.create("tester");
  private final WorkflowSummary summary = new WorkflowSummary();
  private final StepActionProperties properties = new StepActionProperties(30000, 1000);

  @Before
  public void setUp() throws Exception {
    stepInstanceDao = new MaestroStepInstanceDao(dataSource, MAPPER, config, metricRepo);
    actionDao =
        new MaestroStepInstanceActionDao(
            dataSource, MAPPER, config, properties, stepInstanceDao, publisher, metricRepo);
    instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    stepInstance =
        loadObject("fixtures/instances/sample-step-instance-running.json", StepInstance.class);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    actionDao.deleteAction(stepInstance, null);
    when(publisher.publish(any())).thenReturn(Optional.empty());

    summary.setWorkflowId(instance.getWorkflowId());
    summary.setWorkflowInstanceId(instance.getWorkflowInstanceId());
    summary.setWorkflowRunId(instance.getWorkflowRunId());
  }

  @After
  public void tearDown() {
    // clean up step instances
    MaestroTestHelper.removeWorkflowInstance(dataSource, "sample-dag-test-3", 1);
  }

  @Test
  public void testRestartDirectly() {
    RunResponse restartStepInfo = setupRestartStepInfoForRestartDirectly();
    RunRequest runRequest = setupRestartRunRequest();
    RunResponse response = actionDao.restartDirectly(restartStepInfo, runRequest, false);
    Assert.assertEquals("sample-dag-test-3", response.getWorkflowId());
    Assert.assertEquals(1, response.getWorkflowInstanceId());
    Assert.assertEquals(1, response.getWorkflowRunId());
    Assert.assertEquals("job1", response.getStepId());
    Assert.assertEquals(2L, response.getStepAttemptId().longValue());
    Assert.assertEquals(
        "User [tester] take action [RESTART] on the step",
        response.getTimelineEvent().getMessage());
  }

  @Test
  public void testRestartDirectlyWithBlocking() {
    RunResponse restartStepInfo = setupRestartStepInfoForRestartDirectly();
    RunRequest runRequest = setupRestartRunRequest();
    MaestroStepInstanceActionDao spyDao = getSpyActionDao(10000);
    Thread.ofVirtual().start(() -> spyDao.restartDirectly(restartStepInfo, runRequest, true));

    verify(publisher, timeout(3000).times(1)).publish(any(StepInstanceWakeUpEvent.class));
    // assert that the action was saved
    Assert.assertTrue(actionDao.tryGetAction(summary, "job1").isPresent());
    Assert.assertEquals(RESTART, actionDao.tryGetAction(summary, "job1").get().getAction());

    stepInstance.getRuntimeState().setStatus(StepInstance.Status.RUNNING);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    verify(spyDao, timeout(3000).times(1)).deleteAction(any(), any());
    Assert.assertTrue(spyDao.tryGetAction(summary, "job1").isEmpty());
  }

  @Test
  public void testRestartDirectlyWithBlockingAfterTimeout() {
    RunResponse restartStepInfo = setupRestartStepInfoForRestartDirectly();
    RunRequest runRequest = setupRestartRunRequest();
    MaestroStepInstanceActionDao spyDao = getSpyActionDao(300);

    AssertHelper.assertThrows(
        "Should timeout the action",
        MaestroTimeoutException.class,
        "RESTART action for the step [sample-dag-test-3][1][1][job1] is timed out and please retry",
        () -> spyDao.restartDirectly(restartStepInfo, runRequest, true));
    verify(publisher, timeout(3000).times(1)).publish(any(StepInstanceWakeUpEvent.class));
    verify(spyDao, timeout(3000).times(1)).deleteAction(any(), any());
    Assert.assertTrue(spyDao.tryGetAction(summary, "job1").isEmpty());
  }

  private RunResponse setupRestartStepInfoForRestartDirectly() {
    stepInstance.getRuntimeState().setStatus(StepInstance.Status.FATALLY_FAILED);
    stepInstance.getStepRetry().setRetryable(false);
    ((TypedStep) stepInstance.getDefinition()).setFailureMode(FailureMode.FAIL_AFTER_RUNNING);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    return RunResponse.builder().instance(instance).stepId("job1").build();
  }

  private RunRequest setupRestartRunRequest() {
    return RunRequest.builder()
        .requester(user)
        .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
        .stepRunParams(
            Collections.singletonMap(
                "job1",
                Collections.singletonMap(
                    "foo", ParamDefinition.buildParamDefinition("foo", "bar"))))
        .build();
  }

  private MaestroStepInstanceActionDao getSpyActionDao(long timeout) {
    return spy(
        new MaestroStepInstanceActionDao(
            dataSource,
            MAPPER,
            config,
            new StepActionProperties(timeout, 100),
            stepInstanceDao,
            publisher,
            metricRepo));
  }

  @Test
  public void testRestartDirectlyWithTerminatedStep() {
    stepInstance.getRuntimeState().setStatus(StepInstance.Status.FATALLY_FAILED);
    // emulate restarted step finishes
    stepInstance.getRuntimeState().setCreateTime(System.currentTimeMillis() + 3600 * 1000);
    stepInstance.getStepRetry().setRetryable(false);
    ((TypedStep) stepInstance.getDefinition()).setFailureMode(FailureMode.FAIL_AFTER_RUNNING);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    RunResponse restartStepInfo = RunResponse.builder().instance(instance).stepId("job1").build();
    RunRequest runRequest = setupRestartRunRequest();

    RunResponse response = actionDao.restartDirectly(restartStepInfo, runRequest, true);
    Assert.assertEquals("sample-dag-test-3", response.getWorkflowId());
    Assert.assertEquals(1, response.getWorkflowInstanceId());
    Assert.assertEquals(1, response.getWorkflowRunId());
    Assert.assertEquals("job1", response.getStepId());
    Assert.assertEquals(2L, response.getStepAttemptId().longValue());
    Assert.assertEquals(
        "User [tester] take action [RESTART] on the step",
        response.getTimelineEvent().getMessage());
    Mockito.verify(publisher, Mockito.times(1)).publish(any(StepInstanceWakeUpEvent.class));
  }

  @Test
  public void testRestartDirectlyFromAggregatedView() {
    stepInstance.getRuntimeState().setStatus(StepInstance.Status.USER_FAILED);
    stepInstance.getStepRetry().setRetryable(true);
    stepInstance.setWorkflowRunId(2);
    instance
        .getAggregatedInfo()
        .getStepAggregatedViews()
        .put("job1", StepAggregatedView.builder().workflowRunId(2L).build());
    ((TypedStep) stepInstance.getDefinition()).setFailureMode(FailureMode.FAIL_IMMEDIATELY);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    RunResponse restartStepInfo = RunResponse.builder().instance(instance).stepId("job1").build();
    RunRequest runRequest = setupRestartRunRequest();
    RunResponse response = actionDao.restartDirectly(restartStepInfo, runRequest, false);
    Assert.assertEquals("sample-dag-test-3", response.getWorkflowId());
    Assert.assertEquals(1, response.getWorkflowInstanceId());
    Assert.assertEquals(1, response.getWorkflowRunId());
    Assert.assertEquals("job1", response.getStepId());
    Assert.assertEquals(2L, response.getStepAttemptId().longValue());
    Assert.assertEquals(
        "User [tester] take action [RESTART] on the step",
        response.getTimelineEvent().getMessage());
    Mockito.verify(publisher, Mockito.times(1)).publish(any(StepInstanceWakeUpEvent.class));
  }

  @Test
  public void testRestartDirectlyWhileForeachStepRunning() {
    stepInstance.getRuntimeState().setStatus(StepInstance.Status.RUNNING);
    ((TypedStep) stepInstance.getDefinition()).setType(StepType.FOREACH);
    ForeachArtifact artifact = new ForeachArtifact();
    artifact.setForeachWorkflowId("maestro-foreach-wf");
    artifact.setNextLoopIndex(12);
    artifact.setForeachOverview(new ForeachStepOverview());
    artifact.getForeachOverview().addOne(10, WorkflowInstance.Status.FAILED, null);
    artifact.getForeachOverview().refreshDetail();
    stepInstance.setArtifacts(Collections.singletonMap(Artifact.Type.FOREACH.key(), artifact));
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    RunResponse restartStepInfo = RunResponse.builder().instance(instance).stepId("job1").build();
    RunRequest runRequest =
        RunRequest.builder()
            .requester(user)
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode("maestro-foreach-wf", 10, "job2")
                    .addRestartNode("sample-dag-test-3", 1, "job1")
                    .build())
            .build();

    RunResponse response = actionDao.restartDirectly(restartStepInfo, runRequest, false);

    Assert.assertEquals("sample-dag-test-3", response.getWorkflowId());
    Assert.assertEquals(1, response.getWorkflowInstanceId());
    Assert.assertEquals(1, response.getWorkflowRunId());
    Assert.assertEquals("job1", response.getStepId());
    Assert.assertEquals(
        "User [tester] take action [RESTART] on the step",
        response.getTimelineEvent().getMessage());

    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroResourceConflictException.class,
        "There is an ongoing action for this step [sample-dag-test-3][1][1][job1]",
        () -> actionDao.restartDirectly(restartStepInfo, runRequest, false));

    artifact.setNextLoopIndex(9);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroInvalidStatusException.class,
        "Cannot manually RESTART the step [sample-dag-test-3][1][1][job1] as its status [RUNNING] ",
        () -> actionDao.restartDirectly(restartStepInfo, runRequest, false));

    artifact.setNextLoopIndex(10);
    artifact.setPendingAction(ForeachAction.builder().build());
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroResourceConflictException.class,
        "The foreach iteration [10] is not ready to be restarted for foreach step ",
        () -> actionDao.restartDirectly(restartStepInfo, runRequest, false));
  }

  @Test
  public void testGetAction() {
    Optional<StepAction> stepAction = actionDao.tryGetAction(summary, "job1");
    Assert.assertFalse(stepAction.isPresent());
    testRestartDirectly();
    stepAction = actionDao.tryGetAction(summary, "job1");
    Assert.assertTrue(stepAction.isPresent());
    stepAction.ifPresent(
        action -> {
          Assert.assertEquals("sample-dag-test-3", action.getWorkflowId());
          Assert.assertEquals(1, action.getWorkflowInstanceId());
          Assert.assertEquals(1, action.getWorkflowRunId());
          Assert.assertEquals("job1", action.getStepId());
          Assert.assertEquals(RESTART, action.getAction());
          Assert.assertEquals(user, action.getUser());
          Assert.assertEquals(
              Collections.singletonMap("foo", ParamDefinition.buildParamDefinition("foo", "bar")),
              action.getRunParams());
        });
    Mockito.verify(publisher, Mockito.times(1)).publish(any(StepInstanceWakeUpEvent.class));
  }

  @Test
  public void testTryGetActionWithError() throws Exception {
    DataSource dataSource1 = spy(dataSource);
    MaestroStepInstanceActionDao actionDao1 =
        new MaestroStepInstanceActionDao(
            dataSource1, MAPPER, config, properties, stepInstanceDao, publisher, metricRepo);
    doThrow(new RuntimeException("test-exception")).when(dataSource1).getConnection();

    Optional<StepAction> stepAction = actionDao1.tryGetAction(summary, "job1");
    Assert.assertFalse(stepAction.isPresent());
  }

  @Test
  public void testGetActionFromUpstream() {
    SubworkflowInitiator initiator = new SubworkflowInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId("sample-subworkflow-wf");
    parent.setInstanceId(1);
    parent.setRunId(2);
    parent.setStepId("sub-step1");
    WorkflowSummary parentSummary = new WorkflowSummary();
    parentSummary.setWorkflowId("sample-subworkflow-wf");
    parentSummary.setWorkflowInstanceId(1);
    parentSummary.setWorkflowRunId(2);
    UpstreamInitiator.Info root = new UpstreamInitiator.Info();
    root.setWorkflowId("sample-root-wf");
    root.setInstanceId(3);
    root.setRunId(2);
    root.setStepId("root-step1");
    initiator.setAncestors(Arrays.asList(root, parent));
    summary.setInitiator(initiator);
    WorkflowSummary rootSummary = new WorkflowSummary();
    rootSummary.setWorkflowId("sample-root-wf");
    rootSummary.setWorkflowInstanceId(3);
    rootSummary.setWorkflowRunId(2);

    actionDao.terminate(rootSummary, "root-step1", user, RESTART, "test-reason");
    actionDao.terminate(parentSummary, "sub-step1", user, RESTART, "test-reason");
    testRestartDirectly();

    // should ignore root & parent actions
    Optional<StepAction> stepAction = actionDao.tryGetAction(summary, "job1");
    Assert.assertTrue(stepAction.isPresent());
    stepAction.ifPresent(
        action -> {
          Assert.assertEquals("sample-dag-test-3", action.getWorkflowId());
          Assert.assertEquals(1, action.getWorkflowInstanceId());
          Assert.assertEquals(1, action.getWorkflowRunId());
          Assert.assertEquals("job1", action.getStepId());
          Assert.assertEquals(RESTART, action.getAction());
          Assert.assertEquals(user, action.getUser());
          Assert.assertEquals(
              Collections.singletonMap("foo", ParamDefinition.buildParamDefinition("foo", "bar")),
              action.getRunParams());
        });

    // should get root and ignore parent restart action
    actionDao.terminate(rootSummary, "root-step1", user, STOP, "test-reason");
    stepAction = actionDao.tryGetAction(summary, "job1");
    Assert.assertTrue(stepAction.isPresent());
    stepAction.ifPresent(
        action -> {
          Assert.assertEquals("sample-root-wf", action.getWorkflowId());
          Assert.assertEquals(3, action.getWorkflowInstanceId());
          Assert.assertEquals(2, action.getWorkflowRunId());
          Assert.assertEquals("root-step1", action.getStepId());
          Assert.assertEquals(STOP, action.getAction());
          Assert.assertEquals(user, action.getUser());
        });

    // should get parent kill action and ignore root stop action
    actionDao.terminate(parentSummary, "sub-step1", user, KILL, "test-reason");
    stepAction = actionDao.tryGetAction(summary, "job1");
    Assert.assertTrue(stepAction.isPresent());
    stepAction.ifPresent(
        action -> {
          Assert.assertEquals("sample-subworkflow-wf", action.getWorkflowId());
          Assert.assertEquals(1, action.getWorkflowInstanceId());
          Assert.assertEquals(2, action.getWorkflowRunId());
          Assert.assertEquals("sub-step1", action.getStepId());
          Assert.assertEquals(KILL, action.getAction());
          Assert.assertEquals(user, action.getUser());
        });

    // should get parent skip action and ignore root stop action
    actionDao.terminate(parentSummary, "sub-step1", user, SKIP, "test-reason");
    stepAction = actionDao.tryGetAction(summary, "job1");
    Assert.assertTrue(stepAction.isPresent());
    stepAction.ifPresent(
        action -> {
          Assert.assertEquals("sample-subworkflow-wf", action.getWorkflowId());
          Assert.assertEquals(1, action.getWorkflowInstanceId());
          Assert.assertEquals(2, action.getWorkflowRunId());
          Assert.assertEquals("sub-step1", action.getStepId());
          Assert.assertEquals(SKIP, action.getAction());
          Assert.assertEquals(user, action.getUser());
        });

    Assert.assertEquals(1, actionDao.cleanUp("sample-root-wf", 3, 2));
    Assert.assertEquals(1, actionDao.cleanUp("sample-subworkflow-wf", 1, 2));
    Mockito.verify(publisher, Mockito.times(6)).publish(any(StepInstanceWakeUpEvent.class));
  }

  @Test
  public void testInvalidRestart() {
    RunResponse restartStepInfo1 =
        RunResponse.builder().instance(instance).stepId("not-existing").build();
    RunRequest runRequest =
        RunRequest.builder()
            .requester(user)
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .runParams(Collections.emptyMap())
            .build();

    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroBadRequestException.class,
        "Cannot manually RESTART the step [not-existing] because the latest workflow run",
        () -> actionDao.restartDirectly(restartStepInfo1, runRequest, true));

    RunResponse restartStepInfo2 = RunResponse.builder().instance(instance).stepId("job.2").build();
    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroInvalidStatusException.class,
        "Cannot manually RESTART the step [[sample-dag-test-3][1][1]][job.2] as it does not exist",
        () -> actionDao.restartDirectly(restartStepInfo2, runRequest, true));

    RunResponse restartStepInfo3 = RunResponse.builder().instance(instance).stepId("job1").build();
    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroInvalidStatusException.class,
        "Cannot manually RESTART the step [sample-dag-test-3][1][1][job1] as its status [RUNNING] is either non-terminal or complete "
            + "while the workflow instance or its ancestor workflow instances is still non-terminal.",
        () -> actionDao.restartDirectly(restartStepInfo3, runRequest, true));

    RunResponse restartStepInfo4 = RunResponse.builder().instance(instance).stepId("job3").build();
    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroNotFoundException.class,
        "step instance [job3][LATEST] not found (either not created or deleted)",
        () -> actionDao.restartDirectly(restartStepInfo4, runRequest, true));

    stepInstance.getRuntimeState().setStatus(StepInstance.Status.USER_FAILED);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);

    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroInvalidStatusException.class,
        "Cannot manually RESTART the step [sample-dag-test-3][1][1][job1] as the system is still retrying",
        () -> actionDao.restartDirectly(restartStepInfo3, runRequest, true));

    stepInstance.getRuntimeState().setStatus(StepInstance.Status.FATALLY_FAILED);
    stepInstance.getStepRetry().setRetryable(false);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);

    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroInvalidStatusException.class,
        "Cannot manually RESTART the step [sample-dag-test-3][1][1][job1] as step failure mode [IGNORE_FAILURE]",
        () -> actionDao.restartDirectly(restartStepInfo3, runRequest, true));

    ((TypedStep) stepInstance.getDefinition()).setFailureMode(FailureMode.FAIL_AFTER_RUNNING);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    actionDao.restartDirectly(restartStepInfo3, runRequest, false);

    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroResourceConflictException.class,
        "There is an ongoing action for this step [sample-dag-test-3][1][1][job1]",
        () -> actionDao.restartDirectly(restartStepInfo3, runRequest, true));
  }

  @Test
  public void testStop() {
    StepInstanceActionResponse response = actionDao.terminate(instance, "job1", user, STOP, false);
    Assert.assertEquals("sample-dag-test-3", response.getWorkflowId());
    Assert.assertEquals(1, response.getWorkflowInstanceId());
    Assert.assertEquals(1, response.getWorkflowRunId());
    Assert.assertEquals("job1", response.getStepId());
    Assert.assertEquals(1L, response.getStepAttemptId().longValue());
    Assert.assertEquals(
        "User [tester] take action [STOP] on the step due to reason: [manual step instance API call]",
        response.getTimelineEvent().getMessage());
    Mockito.verify(publisher, Mockito.times(1)).publish(any(StepInstanceWakeUpEvent.class));
  }

  @Test
  public void testKill() {
    StepInstanceActionResponse response = actionDao.terminate(instance, "job1", user, KILL, false);
    Assert.assertEquals("sample-dag-test-3", response.getWorkflowId());
    Assert.assertEquals(1, response.getWorkflowInstanceId());
    Assert.assertEquals(1, response.getWorkflowRunId());
    Assert.assertEquals("job1", response.getStepId());
    Assert.assertEquals(1L, response.getStepAttemptId().longValue());
    Assert.assertEquals(
        "User [tester] take action [KILL] on the step due to reason: [manual step instance API call]",
        response.getTimelineEvent().getMessage());
    Mockito.verify(publisher, Mockito.times(1)).publish(any(StepInstanceWakeUpEvent.class));
  }

  @Test
  public void testSkip() {
    StepInstanceActionResponse response = actionDao.terminate(instance, "job1", user, SKIP, false);
    Assert.assertEquals("sample-dag-test-3", response.getWorkflowId());
    Assert.assertEquals(1, response.getWorkflowInstanceId());
    Assert.assertEquals(1, response.getWorkflowRunId());
    Assert.assertEquals("job1", response.getStepId());
    Assert.assertEquals(1L, response.getStepAttemptId().longValue());
    Assert.assertEquals(
        "User [tester] take action [SKIP] on the step due to reason: [manual step instance API call]",
        response.getTimelineEvent().getMessage());
    Mockito.verify(publisher, Mockito.times(1)).publish(any(StepInstanceWakeUpEvent.class));
  }

  @Test
  public void testBypassSignalDependencies() {
    setupStepInstanceForBypassDependencies();
    StepInstanceActionResponse response =
        actionDao.bypassStepDependencies(instance, "job1", user, false);
    Assert.assertEquals("sample-dag-test-3", response.getWorkflowId());
    Assert.assertEquals(1, response.getWorkflowInstanceId());
    Assert.assertEquals(1, response.getWorkflowRunId());
    Assert.assertEquals("job1", response.getStepId());
    Assert.assertEquals(1L, response.getStepAttemptId().longValue());
    Assert.assertEquals(
        "User [tester] take action [BYPASS_STEP_DEPENDENCIES] on the step",
        response.getTimelineEvent().getMessage());
  }

  @Test
  public void testBypassSignalDependenciesWithBlocking() {
    MaestroStepInstanceActionDao spyDao = prepareActionDaoForBypassDependencies(10000);

    Thread.ofVirtual().start(() -> spyDao.bypassStepDependencies(instance, "job1", user, true));
    verify(publisher, timeout(3000).times(1)).publish(any(StepInstanceWakeUpEvent.class));
    // assert that the action was saved
    Assert.assertTrue(actionDao.tryGetAction(summary, "job1").isPresent());
    Assert.assertEquals(
        Actions.StepInstanceAction.BYPASS_STEP_DEPENDENCIES,
        actionDao.tryGetAction(summary, "job1").get().getAction());

    stepInstance.getRuntimeState().setStatus(StepInstance.Status.RUNNING);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    verify(spyDao, timeout(3000).times(1)).deleteAction(any(), any());
    Assert.assertTrue(spyDao.tryGetAction(summary, "job1").isEmpty());
  }

  @Test
  public void testBypassDependenciesWithBlockingAfterTimeout() {
    MaestroStepInstanceActionDao spyDao = prepareActionDaoForBypassDependencies(300);

    AssertHelper.assertThrows(
        "Should timeout the action",
        MaestroTimeoutException.class,
        "bypass-step-dependencies action for the step [sample-dag-test-3][1][1][job1] is timed out",
        () -> spyDao.bypassStepDependencies(instance, "job1", user, true));
    verify(publisher, timeout(3000).times(1)).publish(any(StepInstanceWakeUpEvent.class));
    verify(spyDao, timeout(3000).times(1)).deleteAction(any(), any());
    Assert.assertTrue(spyDao.tryGetAction(summary, "job1").isEmpty());
  }

  private void setupStepInstanceForBypassDependencies() {
    stepInstance.getRuntimeState().setStatus(StepInstance.Status.WAITING_FOR_SIGNALS);
    stepInstance.getStepRetry().setRetryable(false);
    ((TypedStep) stepInstance.getDefinition()).setFailureMode(FailureMode.FAIL_AFTER_RUNNING);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
  }

  private MaestroStepInstanceActionDao prepareActionDaoForBypassDependencies(long timeout) {
    setupStepInstanceForBypassDependencies();
    return getSpyActionDao(timeout);
  }

  @Test
  public void testInvalidBypassSignalDependencies() {
    AssertHelper.assertThrows(
        "Cannot manually RESTART the step",
        MaestroBadRequestException.class,
        "Cannot manually BYPASS_STEP_DEPENDENCIES the step [not-existing] because the latest workflow run",
        () -> actionDao.bypassStepDependencies(instance, "not-existing", user, true));

    stepInstance.getRuntimeState().setStatus(StepInstance.Status.RUNNING);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);

    AssertHelper.assertThrows(
        "Cannot manually bypass the step dependencies",
        MaestroInvalidStatusException.class,
        "Cannot manually bypass-step-dependencies the step as its status [RUNNING] is not waiting for signals",
        () -> actionDao.bypassStepDependencies(instance, "job1", user, true));
  }

  @Test
  public void testInvalidTerminate() {
    AssertHelper.assertThrows(
        "Cannot manually terminate the step",
        MaestroBadRequestException.class,
        "Cannot manually STOP the step [not-existing] because the latest workflow run",
        () -> actionDao.terminate(instance, "not-existing", user, STOP, false));

    AssertHelper.assertThrows(
        "Cannot manually terminate the step",
        MaestroNotFoundException.class,
        "step instance [job.2][LATEST] not found (either not created or deleted)",
        () -> actionDao.terminate(instance, "job.2", user, KILL, false));

    stepInstance.getRuntimeState().setStatus(StepInstance.Status.FATALLY_FAILED);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    AssertHelper.assertThrows(
        "Cannot manually restart the step",
        MaestroInvalidStatusException.class,
        "Cannot manually SKIP the step [sample-dag-test-3][1][1][job1] as it is in a terminal state [FATALLY_FAILED]",
        () -> actionDao.terminate(instance, "job1", user, SKIP, false));

    stepInstance.getRuntimeState().setStatus(StepInstance.Status.RUNNING);
    stepInstanceDao.insertOrUpsertStepInstance(stepInstance, true);
    actionDao.terminate(instance, "job1", user, KILL, false);

    AssertHelper.assertThrows(
        "Cannot manually terminate the step",
        MaestroResourceConflictException.class,
        "There is an ongoing action for this step [sample-dag-test-3][1][1][job1]",
        () -> actionDao.terminate(instance, "job1", user, STOP, false));
  }

  @Test
  public void testDelete() {
    testRestartDirectly();
    // delete any action
    Assert.assertEquals(1, actionDao.deleteAction(stepInstance, null));
    testRestartDirectly();
    // delete matched one
    Assert.assertEquals(1, actionDao.deleteAction(stepInstance, RESTART));
    testRestartDirectly();
    // won't delete unmatched one
    Assert.assertEquals(0, actionDao.deleteAction(stepInstance, STOP));
  }

  @Test
  public void testCleanUp() {
    testRestartDirectly();
    StepInstanceUpdateJobEvent event = new StepInstanceUpdateJobEvent();
    event.setWorkflowId(stepInstance.getWorkflowId());
    event.setWorkflowInstanceId(stepInstance.getWorkflowInstanceId());
    event.setWorkflowRunId(stepInstance.getWorkflowRunId());
    event.setStepId(stepInstance.getStepId());
    Assert.assertEquals(1, actionDao.cleanUp(event));
  }

  @Test
  public void testCleanUpAllSteps() {
    actionDao.terminate(summary, "one-step1", user, STOP, "test-reason");
    actionDao.terminate(summary, "another-step1", user, STOP, "test-reason");
    Assert.assertEquals(
        2,
        actionDao.cleanUp(
            summary.getWorkflowId(), summary.getWorkflowInstanceId(), summary.getWorkflowRunId()));
  }

  @Test
  public void testTerminateStep() {
    testRestartDirectly();
    summary.setMaxGroupNum(10);
    Optional<StepAction> stepAction = actionDao.tryGetAction(summary, "job1");
    Assert.assertTrue(stepAction.isPresent());
    stepAction.ifPresent(
        action -> {
          Assert.assertEquals("sample-dag-test-3", action.getWorkflowId());
          Assert.assertEquals(1, action.getWorkflowInstanceId());
          Assert.assertEquals(1, action.getWorkflowRunId());
          Assert.assertEquals("job1", action.getStepId());
          Assert.assertEquals(RESTART, action.getAction());
          Assert.assertEquals(user, action.getUser());
        });
    // it should overwrite existing RESTART action
    actionDao.terminate(summary, "job1", user, STOP, "test-reason");
    stepAction = actionDao.tryGetAction(summary, "job1");
    Assert.assertTrue(stepAction.isPresent());
    stepAction.ifPresent(
        action -> {
          Assert.assertEquals("sample-dag-test-3", action.getWorkflowId());
          Assert.assertEquals(1, action.getWorkflowInstanceId());
          Assert.assertEquals(1, action.getWorkflowRunId());
          Assert.assertEquals("job1", action.getStepId());
          Assert.assertEquals(STOP, action.getAction());
          Assert.assertEquals(user, action.getUser());
        });
    var event = ArgumentCaptor.forClass(StepInstanceWakeUpEvent.class);
    Mockito.verify(publisher, Mockito.times(2)).publish(event.capture());
    Assert.assertEquals(8, event.getAllValues().getFirst().getMaxGroupNum());
    Assert.assertEquals(10, event.getAllValues().getLast().getMaxGroupNum());
  }

  @Test
  public void testTerminateWorkflow() {
    Assert.assertEquals(
        4, actionDao.terminate(instance, user, Actions.WorkflowInstanceAction.STOP, "test-reason"));
    Assert.assertEquals(
        4,
        actionDao.cleanUp(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance.getWorkflowRunId()));

    instance.setRuntimeOverview(
        WorkflowRuntimeOverview.of(
            4,
            singletonEnumMap(
                StepInstance.Status.FATALLY_FAILED,
                WorkflowStepStatusSummary.of(1L).addStep(Arrays.asList(1L, 2L, 3L, 4L))),
            null));
    Assert.assertEquals(
        4, actionDao.terminate(instance, user, Actions.WorkflowInstanceAction.STOP, "test-reason"));
    Assert.assertEquals(
        4,
        actionDao.cleanUp(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance.getWorkflowRunId()));

    instance.setRuntimeOverview(
        WorkflowRuntimeOverview.of(
            4,
            singletonEnumMap(
                StepInstance.Status.RUNNING,
                WorkflowStepStatusSummary.of(1L).addStep(Arrays.asList(1L, 2L, 3L, 4L))),
            null));
    Assert.assertEquals(
        4, actionDao.terminate(instance, user, Actions.WorkflowInstanceAction.STOP, "test-reason"));
    Assert.assertEquals(
        4,
        actionDao.cleanUp(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance.getWorkflowRunId()));

    instance.setRuntimeOverview(
        WorkflowRuntimeOverview.of(
            4,
            singletonEnumMap(
                StepInstance.Status.NOT_CREATED,
                WorkflowStepStatusSummary.of(1L).addStep(Arrays.asList(1L, 2L, 3L, 4L))),
            null));
    Assert.assertEquals(
        4, actionDao.terminate(instance, user, Actions.WorkflowInstanceAction.STOP, "test-reason"));
    Assert.assertEquals(
        4,
        actionDao.cleanUp(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance.getWorkflowRunId()));

    instance.setRuntimeOverview(
        WorkflowRuntimeOverview.of(
            4,
            singletonEnumMap(
                StepInstance.Status.SUCCEEDED,
                WorkflowStepStatusSummary.of(1L).addStep(Arrays.asList(1L, 2L, 3L, 4L))),
            null));
    Assert.assertEquals(
        3, actionDao.terminate(instance, user, Actions.WorkflowInstanceAction.STOP, "test-reason"));
    Assert.assertEquals(
        3,
        actionDao.cleanUp(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance.getWorkflowRunId()));

    instance.setRuntimeOverview(
        WorkflowRuntimeOverview.of(
            4,
            singletonEnumMap(
                StepInstance.Status.COMPLETED_WITH_ERROR,
                WorkflowStepStatusSummary.of(1L).addStep(Arrays.asList(1L, 2L, 3L, 4L))),
            null));
    Assert.assertEquals(
        3, actionDao.terminate(instance, user, Actions.WorkflowInstanceAction.STOP, "test-reason"));
    Assert.assertEquals(
        3,
        actionDao.cleanUp(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance.getWorkflowRunId()));
    Mockito.verify(publisher, Mockito.times(6)).publish(any(StepInstanceWakeUpEvent.class));
  }
}
