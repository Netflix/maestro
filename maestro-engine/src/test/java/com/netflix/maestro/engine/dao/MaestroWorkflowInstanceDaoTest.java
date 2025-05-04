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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.engine.db.ForeachIterationOverview;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.utils.AggregatedViewHelper;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RunConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowInstanceAggregatedInfo;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.queue.jobevents.TerminateThenRunJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowVersionUpdateJobEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class MaestroWorkflowInstanceDaoTest extends MaestroDaoBaseTest {
  private static final String TEST_WORKFLOW_ID = "sample-dag-test-3";
  private static final String TEST_WORKFLOW_INSTANCE =
      "fixtures/instances/sample-workflow-instance-created.json";
  private static final User TEST_CALLER = User.create("tester");

  private MaestroWorkflowInstanceDao instanceDao;
  private MaestroRunStrategyDao runStrategyDao;
  private WorkflowInstance wfi;
  private final MaestroQueueSystem queueSystem = mock(MaestroQueueSystem.class);

  @Before
  public void setUp() throws Exception {
    instanceDao =
        new MaestroWorkflowInstanceDao(dataSource, MAPPER, config, queueSystem, metricRepo);
    runStrategyDao = new MaestroRunStrategyDao(dataSource, MAPPER, config, queueSystem, metricRepo);

    MaestroWorkflowDao workflowDao =
        new MaestroWorkflowDao(
            dataSource,
            MAPPER,
            config,
            queueSystem,
            mock(TriggerSubscriptionClient.class),
            metricRepo);
    WorkflowDefinition definition =
        loadObject(
            "fixtures/workflows/definition/sample-minimal-wf.json", WorkflowDefinition.class);
    definition.setWorkflow(definition.getWorkflow().toBuilder().id(TEST_WORKFLOW_ID).build());
    Properties properties = new Properties();
    properties.setOwner(User.builder().name("tester").build());
    workflowDao.addWorkflowDefinition(definition, properties);
    verify(queueSystem, times(1)).enqueue(any(), any(WorkflowVersionUpdateJobEvent.class));
    verify(queueSystem, times(1)).notify(any());

    wfi = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi.setWorkflowInstanceId(0L);
    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId("parent");
    initiator.setAncestors(Collections.singletonList(parent));
    wfi.setInitiator(initiator);
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    assertEquals(1, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("8a0bd56f-745f-4a2c-b81b-1b2f89127e73", wfi.getWorkflowUuid());
    verify(queueSystem, times(1)).enqueue(any(), any(StartWorkflowJobEvent.class));
    verify(queueSystem, times(2)).notify(any());
    wfi.setStartTime(null);
    wfi.setModifyTime(null);
    wfi.setRuntimeOverview(null);
    wfi.setExecutionId(null);
    reset(queueSystem);
  }

  @After
  public void tearDown() {
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 1);
    AssertHelper.assertThrows(
        "cannot get non-existing workflow instance",
        MaestroNotFoundException.class,
        "workflow instance [sample-dag-test-3][1][0] not found (either not created or deleted)",
        () -> instanceDao.getLatestWorkflowInstanceRun(TEST_WORKFLOW_ID, 1));
  }

  @Test
  public void testTryTerminateQueuedInstance() throws Exception {
    boolean res =
        instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.STOPPED, "test-reason");
    assertTrue(res);
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    WorkflowInstance updated =
        instanceDao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(WorkflowInstance.Status.STOPPED, updated.getStatus());
    assertEquals(
        "Workflow instance status becomes [STOPPED] due to reason [test-reason]",
        updated.getTimeline().getTimelineEvents().getFirst().getMessage());
    assertNotNull(updated.getEndTime());
    assertNotNull(updated.getModifyTime());
  }

  @Test
  public void testTryTerminateQueuedInstanceNoOp() throws Exception {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(wfi.getWorkflowId());
    summary.setWorkflowInstanceId(wfi.getWorkflowInstanceId());
    summary.setWorkflowRunId(wfi.getWorkflowRunId());
    Optional<Details> details = instanceDao.executeWorkflowInstance(summary, "test-execution-id");
    assertFalse(details.isPresent());
    boolean res =
        instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.STOPPED, "test-reason");
    assertFalse(res);
    verify(queueSystem, times(0)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    WorkflowInstance updated =
        instanceDao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(WorkflowInstance.Status.CREATED, updated.getStatus());
    assertNull(updated.getTimeline());
    assertNull(updated.getEndTime());
    assertNotNull(updated.getModifyTime());
  }

  @Test
  public void testTerminateQueuedInstances() throws Exception {
    WorkflowInstance wfi1 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi1.setWorkflowUuid("wfi1-uuid");
    wfi1.setWorkflowInstanceId(100L);
    WorkflowInstance wfi2 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi2.setWorkflowUuid("wfi2-uuid");
    wfi2.setWorkflowInstanceId(101L);
    WorkflowInstance wfi3 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi3.setWorkflowUuid("wfi3-uuid");
    wfi3.setWorkflowInstanceId(102L);
    Optional<Details> res =
        instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Arrays.asList(wfi1, wfi2, wfi3));
    assertFalse(res.isPresent());
    verify(queueSystem, times(1)).enqueue(any(), any(TerminateThenRunJobEvent.class));
    verify(queueSystem, times(1)).notify(any());

    int cnt =
        instanceDao.terminateQueuedInstances(
            TEST_WORKFLOW_ID, 3, WorkflowInstance.Status.STOPPED, "test-reason");
    assertEquals(3, cnt);
    verify(queueSystem, times(1)).enqueue(any(), any(WorkflowInstanceUpdateJobEvent.class));
    verify(queueSystem, times(2)).notify(any());
    cnt =
        instanceDao.terminateQueuedInstances(
            TEST_WORKFLOW_ID, 2, WorkflowInstance.Status.FAILED, "test-reason");
    assertEquals(1, cnt);
    verify(queueSystem, times(2)).enqueue(any(), any(WorkflowInstanceUpdateJobEvent.class));
    verify(queueSystem, times(3)).notify(any());
    cnt =
        instanceDao.terminateQueuedInstances(
            TEST_WORKFLOW_ID, 1, WorkflowInstance.Status.STOPPED, "test-reason");
    assertEquals(0, cnt);
    verify(queueSystem, times(2)).enqueue(any(), any(WorkflowInstanceUpdateJobEvent.class));
    verify(queueSystem, times(4)).notify(any());
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 100);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 101);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 102);
  }

  @Test
  public void testTerminateQueuedInstancesNoOp() throws Exception {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(wfi.getWorkflowId());
    summary.setWorkflowInstanceId(wfi.getWorkflowInstanceId());
    summary.setWorkflowRunId(wfi.getWorkflowRunId());
    instanceDao.executeWorkflowInstance(summary, "test_execution_id");
    int cnt =
        instanceDao.terminateQueuedInstances(
            TEST_WORKFLOW_ID, 2, WorkflowInstance.Status.STOPPED, "test-reason");
    assertEquals(0, cnt);
    verify(queueSystem, times(0)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(null);
  }

  @Test
  public void testTerminateRunningInstances() throws Exception {
    WorkflowInstance wfi1 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi1.setWorkflowUuid("wfi1-uuid");
    wfi1.setWorkflowInstanceId(100L);
    WorkflowInstance wfi2 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi2.setWorkflowUuid("wfi2-uuid");
    wfi2.setWorkflowInstanceId(101L);
    WorkflowInstance wfi3 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi3.setWorkflowUuid("wfi3-uuid");
    wfi3.setWorkflowInstanceId(102L);
    Optional<Details> res =
        instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Arrays.asList(wfi1, wfi2, wfi3));
    assertFalse(res.isPresent());
    verify(queueSystem, times(1)).enqueue(any(), any(TerminateThenRunJobEvent.class));
    verify(queueSystem, times(1)).notify(any());
    int cnt =
        instanceDao.terminateRunningInstances(
            TEST_WORKFLOW_ID, 5, Actions.WorkflowInstanceAction.STOP, TEST_CALLER, "test-reason");
    assertEquals(0, cnt);
    verify(queueSystem, times(0)).enqueueOrThrow(any(TerminateThenRunJobEvent.class));

    Stream.of(wfi1, wfi2, wfi3)
        .forEach(
            instance -> {
              WorkflowSummary summary = new WorkflowSummary();
              summary.setWorkflowId(instance.getWorkflowId());
              summary.setWorkflowInstanceId(instance.getWorkflowInstanceId());
              summary.setWorkflowRunId(instance.getWorkflowRunId());
              instanceDao.executeWorkflowInstance(
                  summary, "test_execution_id" + instance.getWorkflowInstanceId());
            });
    cnt =
        instanceDao.terminateRunningInstances(
            TEST_WORKFLOW_ID, 5, Actions.WorkflowInstanceAction.STOP, TEST_CALLER, "test-reason");
    assertEquals(3, cnt);
    verify(queueSystem, times(1)).enqueueOrThrow(any(TerminateThenRunJobEvent.class));

    // async termination
    cnt =
        instanceDao.terminateRunningInstances(
            TEST_WORKFLOW_ID, 2, Actions.WorkflowInstanceAction.KILL, TEST_CALLER, "test-reason");
    assertEquals(3, cnt);
    verify(queueSystem, times(3)).enqueueOrThrow(any(TerminateThenRunJobEvent.class));

    cnt =
        instanceDao.terminateRunningInstances(
            TEST_WORKFLOW_ID, 1, Actions.WorkflowInstanceAction.STOP, TEST_CALLER, "test-reason");
    assertEquals(3, cnt);
    verify(queueSystem, times(6)).enqueueOrThrow(any(TerminateThenRunJobEvent.class));

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 100);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 101);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 102);
  }

  @Test
  public void testTerminateRunningInstancesNoOp() throws Exception {
    int cnt =
        instanceDao.terminateRunningInstances(
            TEST_WORKFLOW_ID, 2, Actions.WorkflowInstanceAction.STOP, TEST_CALLER, "test-reason");
    assertEquals(0, cnt);
    verify(queueSystem, times(0)).enqueue(any(), any());
    verify(queueSystem, times(0)).notify(any());
  }

  @Test
  public void testRunWorkflowInstances() throws Exception {
    WorkflowInstance wfi1 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi1.setWorkflowUuid("wfi1-uuid");
    wfi1.setWorkflowInstanceId(100L);
    WorkflowInstance wfi2 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi2.setWorkflowUuid("wfi2-uuid");
    wfi2.setWorkflowInstanceId(101L);
    WorkflowInstance wfi3 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi3.setWorkflowUuid("wfi3-uuid");
    wfi3.setWorkflowInstanceId(102L);

    Optional<Details> res =
        instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Arrays.asList(wfi1, wfi2, wfi3));
    assertFalse(res.isPresent());
    assertEquals(100, wfi1.getWorkflowInstanceId());
    assertEquals(1, wfi1.getWorkflowRunId());
    assertEquals(WorkflowInstance.Status.CREATED, wfi1.getStatus());
    assertEquals(101, wfi2.getWorkflowInstanceId());
    assertEquals(1, wfi2.getWorkflowRunId());
    assertEquals(WorkflowInstance.Status.CREATED, wfi2.getStatus());
    assertEquals(102, wfi3.getWorkflowInstanceId());
    assertEquals(1, wfi3.getWorkflowRunId());
    assertEquals(WorkflowInstance.Status.CREATED, wfi3.getStatus());
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());

    WorkflowInstance actual =
        instanceDao.getLatestWorkflowInstanceRun(TEST_WORKFLOW_ID, wfi3.getWorkflowInstanceId());
    assertEquals(102, actual.getWorkflowInstanceId());
    assertEquals(1, actual.getWorkflowRunId());
    assertEquals(WorkflowInstance.Status.CREATED, actual.getStatus());

    reset(queueSystem);
    wfi3.setWorkflowUuid("wfi-uuid");
    wfi3.setWorkflowInstanceId(103L);

    doAnswer(
            (Answer<Void>)
                invocation -> {
                  TerminateThenRunJobEvent jobEvent =
                      (TerminateThenRunJobEvent) invocation.getArguments()[1];
                  assertEquals(TEST_WORKFLOW_ID, jobEvent.getWorkflowId());
                  assertEquals(1, jobEvent.getRunAfter().size());
                  assertEquals(103, jobEvent.getRunAfter().getFirst().getInstanceId());
                  assertEquals("wfi-uuid", jobEvent.getRunAfter().getFirst().getUuid());
                  return null;
                })
        .when(queueSystem)
        .enqueue(any(), any());

    res = instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Arrays.asList(wfi1, wfi2, wfi3));
    assertFalse(res.isPresent());
    actual =
        instanceDao.getLatestWorkflowInstanceRun(TEST_WORKFLOW_ID, wfi3.getWorkflowInstanceId());
    assertEquals(103, actual.getWorkflowInstanceId());
    assertEquals(1, actual.getWorkflowRunId());
    assertEquals(WorkflowInstance.Status.CREATED, actual.getStatus());
    verify(queueSystem, times(1)).notify(any());

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 100);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 101);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 102);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 103);
  }

  @Test
  public void testRunWorkflowInstancesRetry() throws Exception {
    WorkflowInstance wfi1 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi1.setWorkflowUuid("wfi1-uuid"); // same instance id
    Optional<Details> res =
        instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Collections.singletonList(wfi1));
    assertFalse(res.isPresent());
    verify(queueSystem, times(0)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());

    WorkflowInstance wfi2 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class); // same uuid
    wfi2.setWorkflowInstanceId(2);
    res = instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Collections.singletonList(wfi2));
    assertTrue(res.isPresent());
    assertEquals(
        "ERROR: failed creating workflow instance batch with an error", res.get().getMessage());
    assertTrue(
        res.get()
            .getCause()
            .getMessage()
            .contains("duplicate key value violates unique constraint \"workflow_unique_index\""));

    AssertHelper.assertThrows(
        "cannot get non-existing workflow instance",
        MaestroNotFoundException.class,
        "workflow instance [sample-dag-test-3][2][0] not found (either not created or deleted)",
        () -> instanceDao.getLatestWorkflowInstanceRun(TEST_WORKFLOW_ID, 2));
  }

  @Test
  public void testRunWorkflowInstancesToUpdateAncestorRuns() throws Exception {
    boolean res =
        instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.FAILED, "test-reason");
    assertTrue(res);

    WorkflowInstance wfi1 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi1.setWorkflowUuid("wfi1-uuid");
    wfi1.setWorkflowRunId(3L);
    wfi1.setRunConfig(new RunConfig());
    wfi1.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);

    Optional<Details> ret =
        instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Collections.singletonList(wfi1));
    assertFalse(ret.isPresent());

    String rawStatus = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 1L, 1L);
    assertEquals("FAILED_2", rawStatus);
    WorkflowInstance.Status status =
        instanceDao.getWorkflowInstanceStatus(TEST_WORKFLOW_ID, 1L, 1L);
    assertEquals(WorkflowInstance.Status.FAILED, status);
    status = instanceDao.getWorkflowInstanceStatus(TEST_WORKFLOW_ID, 1L, 3L);
    assertEquals(WorkflowInstance.Status.CREATED, status);
  }

  @Test
  public void testUpdateWorkflowInstanceToEnd() throws Exception {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(wfi.getWorkflowId());
    summary.setWorkflowInstanceId(wfi.getWorkflowInstanceId());
    summary.setWorkflowRunId(wfi.getWorkflowRunId());
    instanceDao.executeWorkflowInstance(summary, "test_execution_id");
    WorkflowRuntimeOverview overview =
        WorkflowRuntimeOverview.of(
            4,
            singletonEnumMap(StepInstance.Status.SUCCEEDED, WorkflowStepStatusSummary.of(4L)),
            null);
    Optional<Details> result =
        instanceDao.updateWorkflowInstance(
            summary,
            overview,
            null,
            WorkflowInstance.Status.SUCCEEDED,
            12345,
            mock(MaestroJobEvent.class));
    assertFalse(result.isPresent());
    WorkflowInstance latestRun =
        instanceDao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(overview, latestRun.getRuntimeOverview());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, latestRun.getStatus());
    assertEquals(12345L, latestRun.getEndTime().longValue());
    assertEquals("test_execution_id", latestRun.getExecutionId());
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
  }

  @Test
  public void testUpdateWorkflowInstanceToBegin() throws Exception {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(wfi.getWorkflowId());
    summary.setWorkflowInstanceId(wfi.getWorkflowInstanceId());
    summary.setWorkflowRunId(wfi.getWorkflowRunId());
    instanceDao.executeWorkflowInstance(summary, "test_execution_id");
    WorkflowRuntimeOverview overview =
        WorkflowRuntimeOverview.of(
            4,
            singletonEnumMap(StepInstance.Status.RUNNING, WorkflowStepStatusSummary.of(4L)),
            null);
    Optional<Details> result =
        instanceDao.updateWorkflowInstance(
            summary,
            overview,
            null,
            WorkflowInstance.Status.IN_PROGRESS,
            12345,
            mock(MaestroJobEvent.class));
    assertFalse(result.isPresent());
    WorkflowInstance latestRun =
        instanceDao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(overview, latestRun.getRuntimeOverview());
    assertEquals(WorkflowInstance.Status.IN_PROGRESS, latestRun.getStatus());
    assertEquals(12345L, latestRun.getStartTime().longValue());
    assertEquals("test_execution_id", latestRun.getExecutionId());
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
  }

  @Test
  public void testUpdateRuntimeOverview() throws Exception {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(wfi.getWorkflowId());
    summary.setWorkflowInstanceId(wfi.getWorkflowInstanceId());
    summary.setWorkflowRunId(wfi.getWorkflowRunId());
    instanceDao.executeWorkflowInstance(summary, "test_execution_id");
    WorkflowRuntimeOverview overview =
        WorkflowRuntimeOverview.of(
            4,
            singletonEnumMap(StepInstance.Status.RUNNING, WorkflowStepStatusSummary.of(4L)),
            null);
    Timeline timeline =
        new Timeline(Collections.singletonList(TimelineLogEvent.info("hello world")));
    Optional<Details> result = instanceDao.updateRuntimeOverview(summary, overview, timeline);
    assertFalse(result.isPresent());
    WorkflowInstance latestRun =
        instanceDao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(overview, latestRun.getRuntimeOverview());
    assertEquals(timeline, latestRun.getTimeline());
    assertEquals("test_execution_id", latestRun.getExecutionId());
    verify(queueSystem, times(0)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
  }

  @Test
  public void testInvalidWorkflowInstanceUpdate() {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(TEST_WORKFLOW_ID);
    summary.setWorkflowInstanceId(1);
    summary.setWorkflowRunId(2);
    Optional<Details> result = instanceDao.updateRuntimeOverview(summary, null, null);
    assertTrue(result.isPresent());
    assertEquals(
        "ERROR: updated [0] (expecting 1) rows for workflow instance [sample-dag-test-3][1][2]",
        result.get().getMessage());
    result = instanceDao.updateRuntimeOverview(null, null, null);
    assertTrue(result.isPresent());
    assertEquals(
        "ERROR: failed updating Runtime Maestro Workflow with an error", result.get().getMessage());
  }

  @Test
  public void testGetWorkflowInstanceView() {
    instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.FAILED, "kill the test");
    wfi.setWorkflowUuid("test-uuid");
    wfi.setWorkflowRunId(0L);
    wfi.setRunConfig(new RunConfig());
    wfi.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    assertEquals(1, wfi.getWorkflowInstanceId());
    assertEquals(2, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());

    WorkflowInstance instanceRun =
        instanceDao.getWorkflowInstance(
            wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), Constants.LATEST_INSTANCE_RUN, true);
    instanceRun.setModifyTime(null);
    WorkflowInstanceAggregatedInfo aggregatedInfo =
        AggregatedViewHelper.computeAggregatedView(wfi, true);
    wfi.setAggregatedInfo(aggregatedInfo);
    assertEquals(wfi, instanceRun);
  }

  @Test
  public void testGetWorkflowInstance() {
    WorkflowInstance instanceRun =
        instanceDao.getWorkflowInstance(
            wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), Constants.LATEST_INSTANCE_RUN, false);
    instanceRun.setModifyTime(null);
    assertEquals(wfi, instanceRun);

    instanceRun =
        instanceDao.getWorkflowInstance(
            wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), "1", false);
    instanceRun.setModifyTime(null);
    assertEquals(wfi, instanceRun);
  }

  @Test
  public void testGetWorkflowInstanceRun() {
    WorkflowInstance instanceRun =
        instanceDao.getWorkflowInstanceRun(
            wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), wfi.getWorkflowRunId());
    instanceRun.setModifyTime(null);
    assertEquals(wfi, instanceRun);
  }

  @Test
  public void testGetWorkflowInstanceRunByUuid() {
    WorkflowInstance instanceRun =
        instanceDao.getWorkflowInstanceRunByUuid(wfi.getWorkflowId(), wfi.getWorkflowUuid());
    instanceRun.setModifyTime(null);
    assertEquals(wfi, instanceRun);
  }

  @Test
  public void testGetLatestWorkflowInstanceRun() {
    instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.FAILED, "kill the test");
    wfi.setWorkflowUuid("test-uuid");
    wfi.setWorkflowRunId(0L);
    wfi.setRunConfig(new RunConfig());
    wfi.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    assertEquals(1, wfi.getWorkflowInstanceId());
    assertEquals(2, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());

    WorkflowInstance instanceRun =
        instanceDao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    instanceRun.setModifyTime(null);
    assertEquals(wfi, instanceRun);
  }

  @Test
  public void testWorkflowInstanceMetadataForWorkflowInstancesLatestRun() {
    instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.FAILED, "kill the test");
    wfi.setWorkflowUuid("test-uuid");
    wfi.setWorkflowRunId(0L);
    wfi.setRunConfig(new RunConfig());
    wfi.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    assertEquals(1, wfi.getWorkflowInstanceId());
    assertEquals(2, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());

    List<WorkflowInstance> workflowInstances =
        instanceDao.getWorkflowInstancesWithLatestRun(wfi.getWorkflowId(), 1, 1, false);
    WorkflowInstance instanceRun = workflowInstances.getFirst();
    instanceRun.setModifyTime(null);
    assertEquals(wfi, instanceRun);
  }

  @Test
  public void testGetWorkflowInstanceLatestRuns() throws Exception {
    initializeForGetWorkflowInstancesLatestRun();

    // pagination call to get all the latest runs workflow instance id's in a particular workflow
    // id, null cursor
    List<WorkflowInstance> workflows =
        instanceDao.getWorkflowInstancesWithLatestRun(TEST_WORKFLOW_ID, 1, 100, false);

    workflows.sort(Comparator.comparing(WorkflowInstance::getWorkflowInstanceId).reversed());
    assertNotNull(workflows);
    assertEquals(10, workflows.size());
    for (int i = 0; i < 10; i++) {
      WorkflowInstance instance = workflows.get(i);
      assertNotNull(instance);
      assertEquals(TEST_WORKFLOW_ID, instance.getWorkflowId());
      assertEquals(10 - i, instance.getWorkflowInstanceId());
      assertEquals(109 - i, instance.getWorkflowRunId());
    }

    // pagination NEXT call to get a subset of pages, which fill the entire page request
    workflows = instanceDao.getWorkflowInstancesWithLatestRun(TEST_WORKFLOW_ID, 2, 6, false);
    workflows.sort(Comparator.comparing(WorkflowInstance::getWorkflowInstanceId).reversed());
    assertNotNull(workflows);
    assertEquals(5, workflows.size());
    for (int i = 0; i < workflows.size(); i++) {
      WorkflowInstance instance = workflows.get(i);
      assertNotNull(instance);
      assertEquals(TEST_WORKFLOW_ID, instance.getWorkflowId());
      assertEquals(6 - i, instance.getWorkflowInstanceId());
      assertEquals(105 - i, instance.getWorkflowRunId());
    }

    // pagination NEXT call to get a subset of pages, which fill portion of the page requested
    workflows = instanceDao.getWorkflowInstancesWithLatestRun(TEST_WORKFLOW_ID, 1, 3, false);
    workflows.sort(Comparator.comparing(WorkflowInstance::getWorkflowInstanceId).reversed());
    assertNotNull(workflows);
    assertEquals(3, workflows.size());
    for (int i = 0; i < workflows.size(); i++) {
      WorkflowInstance instance = workflows.get(i);
      assertNotNull(instance);
      assertEquals(TEST_WORKFLOW_ID, instance.getWorkflowId());
      assertEquals(3 - i, instance.getWorkflowInstanceId());
      assertEquals(102 - i, instance.getWorkflowRunId());
    }

    // batch call with nothing
    workflows = instanceDao.getWorkflowInstancesWithLatestRun(TEST_WORKFLOW_ID, 0, 0, false);
    assertNotNull(workflows);
    assertTrue(workflows.isEmpty());

    // non-existing workflow id
    workflows =
        instanceDao.getWorkflowInstancesWithLatestRun("sample-dag-test-random", 1, 1, false);
    assertNotNull(workflows);
    assertTrue(workflows.isEmpty());

    cleanupForGetWorkflowInstancesLatestRun();
  }

  @Test
  public void testTryUnblockFailedWorkflowInstance() throws Exception {
    int cnt =
        instanceDao.terminateQueuedInstances(
            TEST_WORKFLOW_ID, 2, WorkflowInstance.Status.FAILED, "test-reason");
    assertEquals(1L, cnt);
    String status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 1L, 1L);
    assertEquals("FAILED", status);
    boolean ret = instanceDao.tryUnblockFailedWorkflowInstance(TEST_WORKFLOW_ID, 1L, 1L, null);
    assertTrue(ret);
    status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 1L, 1L);
    assertEquals("FAILED_1", status);
    verify(queueSystem, times(1)).enqueue(any(), any(StartWorkflowJobEvent.class));
    verify(queueSystem, times(2)).notify(any());
  }

  @Test
  public void testTryUnblockFailedWorkflowInstanceNoop() throws Exception {
    int cnt =
        instanceDao.terminateQueuedInstances(
            TEST_WORKFLOW_ID, 2, WorkflowInstance.Status.STOPPED, "test-reason");
    assertEquals(1L, cnt);
    String status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 1L, 1L);
    assertEquals("STOPPED", status);
    boolean ret = instanceDao.tryUnblockFailedWorkflowInstance(TEST_WORKFLOW_ID, 1L, 1L, null);
    assertFalse(ret);
    status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 1L, 1L);
    assertEquals("STOPPED", status);
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(2)).notify(any());
  }

  @Test
  public void testTryUnblockFailedWorkflowInstances() throws Exception {
    wfi.setWorkflowUuid("test-uuid");
    wfi.setWorkflowInstanceId(0L);
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    int cnt =
        instanceDao.terminateQueuedInstances(
            TEST_WORKFLOW_ID, 2, WorkflowInstance.Status.FAILED, "test-reason");
    assertEquals(2L, cnt);
    String status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 1L, 1L);
    assertEquals("FAILED", status);
    status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 2L, 1L);
    assertEquals("FAILED", status);
    verify(queueSystem, times(2)).enqueue(any(), any());
    verify(queueSystem, times(2)).notify(any());

    int ret = instanceDao.tryUnblockFailedWorkflowInstances(TEST_WORKFLOW_ID, 1, null);
    assertEquals(2, ret);
    status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 1L, 1L);
    assertEquals("FAILED_1", status);
    status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 2L, 1L);
    assertEquals("FAILED_1", status);
    verify(queueSystem, times(3)).enqueue(any(), any());
    verify(queueSystem, times(3)).notify(any());

    ret = instanceDao.tryUnblockFailedWorkflowInstances(TEST_WORKFLOW_ID, 2, null);
    assertEquals(0, ret);
    status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 1L, 1L);
    assertEquals("FAILED_1", status);
    status = instanceDao.getWorkflowInstanceRawStatus(TEST_WORKFLOW_ID, 2L, 1L);
    assertEquals("FAILED_1", status);
    verify(queueSystem, times(3)).enqueue(any(), any());
    verify(queueSystem, times(4)).notify(any());

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testGetNonExistingWorkflowInstanceRun() {
    AssertHelper.assertThrows(
        "throw errors if getting non-existing workflow instance",
        MaestroNotFoundException.class,
        "workflow instance [sample-dag-test-3][1][2] not found (either not created or deleted)",
        () ->
            instanceDao.getWorkflowInstanceRun(
                wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), wfi.getWorkflowRunId() + 1));

    AssertHelper.assertThrows(
        "throw errors if getting non-existing workflow instance",
        MaestroNotFoundException.class,
        "workflow instance [not-existing-workflow-id][1][0] not found (either not created or deleted)",
        () ->
            instanceDao.getLatestWorkflowInstanceRun(
                "not-existing-workflow-id", wfi.getWorkflowInstanceId()));

    AssertHelper.assertThrows(
        "throw errors if getting non-existing workflow instance",
        MaestroNotFoundException.class,
        "workflow instance [sample-dag-test-3][2][0] not found (either not created or deleted)",
        () ->
            instanceDao.getLatestWorkflowInstanceRun(
                wfi.getWorkflowId(), wfi.getWorkflowInstanceId() + 1));
  }

  @Test
  public void testGetRunningForeachIterationOverview() {
    List<ForeachIterationOverview> stats =
        instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 1, 0, false);
    checkSingletonStats(stats, 1L, WorkflowInstance.Status.CREATED);
    stats = instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 1, 0, false);
    checkSingletonStats(stats, 1L, WorkflowInstance.Status.CREATED);
    boolean res =
        instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.STOPPED, "test-reason");
    assertTrue(res);
    stats = instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 1, 0, false);
    checkSingletonStats(stats, 1L, WorkflowInstance.Status.STOPPED);

    stats = instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 1, 2, false);
    assertTrue(stats.isEmpty());
  }

  @Test
  public void testGetRestartedForeachIterationOverview() throws Exception {
    List<ForeachIterationOverview> stats =
        instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 0, 0, false);
    assertTrue(stats.isEmpty());
    stats = instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 0, 0, true);
    checkSingletonStats(stats, 1L, WorkflowInstance.Status.CREATED);

    wfi.setWorkflowRunId(2);
    wfi.setStatus(WorkflowInstance.Status.IN_PROGRESS);
    wfi.setWorkflowUuid("uuid-2");
    Optional<Details> res =
        instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Collections.singletonList(wfi));
    assertFalse(res.isPresent());
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());

    stats = instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 0, 0, true);
    checkSingletonStats(stats, 1L, WorkflowInstance.Status.IN_PROGRESS);
    assertNull(stats.getFirst().getRollupOverview());

    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(wfi.getWorkflowId());
    summary.setWorkflowInstanceId(wfi.getWorkflowInstanceId());
    summary.setWorkflowRunId(wfi.getWorkflowRunId());
    WorkflowRuntimeOverview overview =
        WorkflowRuntimeOverview.of(
            4,
            singletonEnumMap(StepInstance.Status.SUCCEEDED, WorkflowStepStatusSummary.of(4L)),
            new WorkflowRollupOverview());
    res =
        instanceDao.updateWorkflowInstance(
            summary, overview, null, WorkflowInstance.Status.SUCCEEDED, 12345, null);
    assertFalse(res.isPresent());
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(2)).notify(any());

    stats = instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 1, 0, false);
    checkSingletonStats(stats, 1L, WorkflowInstance.Status.CREATED);
    assertNull(stats.getFirst().getRollupOverview());
    stats = instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 2, 0, false);
    checkSingletonStats(stats, 1L, WorkflowInstance.Status.SUCCEEDED);
    assertNotNull(stats.getFirst().getRollupOverview());

    stats = instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 0, 0, true);
    checkSingletonStats(stats, 1L, WorkflowInstance.Status.SUCCEEDED);
    assertNotNull(stats.getFirst().getRollupOverview());

    stats = instanceDao.getForeachIterationOverviewWithCheckpoint(wfi.getWorkflowId(), 0, 2, true);
    assertTrue(stats.isEmpty());
  }

  private void checkSingletonStats(
      List<ForeachIterationOverview> stats, long instanceId, WorkflowInstance.Status status) {
    assertEquals(1, stats.size());
    assertEquals(1, instanceId);
    assertEquals(status, stats.getFirst().getStatus());
  }

  @Test
  public void testGetLatestWorkflowInstanceStatus() {
    WorkflowInstance.Status status =
        instanceDao.getLatestWorkflowInstanceStatus(
            wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(WorkflowInstance.Status.CREATED, status);
    boolean res =
        instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.FAILED, "test-reason");
    assertTrue(res);
    status =
        instanceDao.getLatestWorkflowInstanceStatus(
            wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(WorkflowInstance.Status.FAILED, status);
  }

  @Test
  public void testGetWorkflowInstanceStatus() {
    WorkflowInstance.Status status =
        instanceDao.getWorkflowInstanceStatus(
            wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), wfi.getWorkflowRunId());
    assertEquals(WorkflowInstance.Status.CREATED, status);
    boolean res =
        instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.FAILED, "test-reason");
    assertTrue(res);
    status =
        instanceDao.getWorkflowInstanceStatus(
            wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), wfi.getWorkflowRunId());
    assertEquals(WorkflowInstance.Status.FAILED, status);
  }

  @Test
  public void testGetLargestForeachRunIdFromAncestors() throws Exception {
    assertEquals(1L, instanceDao.getLargestForeachRunIdFromRuns(TEST_WORKFLOW_ID));

    WorkflowInstance wfi2 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi2.setWorkflowUuid("wfi2-uuid");
    wfi2.setWorkflowInstanceId(101L);
    wfi2.setWorkflowRunId(2L);
    wfi2.setInitiator(new ForeachInitiator());
    Optional<Details> ret =
        instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Collections.singletonList(wfi2));
    assertFalse(ret.isPresent());

    assertEquals(2L, instanceDao.getLargestForeachRunIdFromRuns(TEST_WORKFLOW_ID));
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 101);
  }

  @Test
  public void testExistWorkflowIdInInstances() {
    assertTrue(instanceDao.existWorkflowIdInInstances(wfi.getWorkflowId()));
    assertFalse(instanceDao.existWorkflowIdInInstances("not-existing"));
  }

  private void cleanupForGetWorkflowInstancesLatestRun() {
    // delete inserted extra instances
    for (int i = 1; i < 10; i++) {
      MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, i + 1);
    }
  }

  private void initializeForGetWorkflowInstancesLatestRun() throws Exception {
    List<WorkflowInstance> instances = new ArrayList<>();

    for (int i = 1; i < 10; i++) {
      WorkflowInstance wfi = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
      wfi.setWorkflowUuid("wfi-uuid-" + i);
      wfi.setWorkflowInstanceId(i + 1);
      instances.add(wfi);
    }

    // insert with new run id's
    for (int i = 0; i < 10; i++) {
      WorkflowInstance wfi = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
      wfi.setWorkflowUuid("wfi-uuid1-" + i);
      wfi.setWorkflowInstanceId(i + 1);
      wfi.setWorkflowRunId(10 + i);
      instances.add(wfi);
    }

    // insert with even bigger run id's
    for (int i = 0; i < 10; i++) {
      WorkflowInstance wfi = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
      wfi.setWorkflowUuid("wfi-uuid2-" + i);
      wfi.setWorkflowInstanceId(i + 1);
      wfi.setWorkflowRunId(100 + i);
      instances.add(wfi);
    }

    instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, instances);
    verify(queueSystem, times(1)).enqueue(any(), any(TerminateThenRunJobEvent.class));
    verify(queueSystem, times(1)).notify(any());
  }

  @Test
  public void testGetBatchForeachLatestRunRollupForIterations() throws Exception {
    boolean res =
        instanceDao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.FAILED, "test-reason");
    assertTrue(res);

    WorkflowRollupOverview rollup1 = new WorkflowRollupOverview();
    rollup1.setTotalLeafCount(5);

    WorkflowRollupOverview rollup2 = new WorkflowRollupOverview();
    rollup2.setTotalLeafCount(10);

    WorkflowRollupOverview rollup3 = new WorkflowRollupOverview();
    rollup3.setTotalLeafCount(20);

    WorkflowInstance wfi1 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi1.setWorkflowUuid("wfi1-uuid");
    wfi1.setWorkflowRunId(3L);
    wfi1.setRunConfig(new RunConfig());
    wfi1.setInitiator(new ForeachInitiator());
    wfi1.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);

    Optional<Details> ret =
        instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Collections.singletonList(wfi1));
    assertFalse(ret.isPresent());

    WorkflowInstance wfi2 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi2.setWorkflowUuid("wfi2-uuid");
    wfi2.setWorkflowInstanceId(101L);
    wfi2.setWorkflowRunId(2L);
    wfi2.setInitiator(new ForeachInitiator());
    WorkflowInstance wfi3 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi3.setWorkflowUuid("wfi3-uuid");
    wfi3.setWorkflowInstanceId(102L);
    wfi3.setWorkflowRunId(2L);
    wfi3.setInitiator(new ForeachInitiator());
    WorkflowInstance wfi5 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi5.setWorkflowUuid("wfi5-uuid");
    wfi5.setWorkflowInstanceId(102L);
    wfi5.setWorkflowRunId(3L);
    wfi5.setInitiator(new ForeachInitiator());

    ret = instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, Arrays.asList(wfi2, wfi3, wfi5));
    assertFalse(ret.isPresent());

    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(wfi.getWorkflowId());
    summary.setWorkflowInstanceId(101L);
    summary.setWorkflowRunId(2L);
    WorkflowRuntimeOverview overview =
        WorkflowRuntimeOverview.of(
            5,
            singletonEnumMap(StepInstance.Status.SUCCEEDED, WorkflowStepStatusSummary.of(5L)),
            rollup1);
    Optional<Details> details =
        instanceDao.updateWorkflowInstance(
            summary, overview, null, WorkflowInstance.Status.SUCCEEDED, 12345, null);
    assertFalse(details.isPresent());

    summary = new WorkflowSummary();
    summary.setWorkflowId(wfi.getWorkflowId());
    summary.setWorkflowInstanceId(102L);
    summary.setWorkflowRunId(2L);
    overview =
        WorkflowRuntimeOverview.of(
            10,
            singletonEnumMap(StepInstance.Status.SUCCEEDED, WorkflowStepStatusSummary.of(10L)),
            rollup2);
    details =
        instanceDao.updateWorkflowInstance(
            summary, overview, null, WorkflowInstance.Status.SUCCEEDED, 12345, null);
    assertFalse(details.isPresent());

    summary = new WorkflowSummary();
    summary.setWorkflowId(wfi.getWorkflowId());
    summary.setWorkflowInstanceId(102L);
    summary.setWorkflowRunId(3L);
    overview =
        WorkflowRuntimeOverview.of(
            20,
            singletonEnumMap(StepInstance.Status.SUCCEEDED, WorkflowStepStatusSummary.of(20L)),
            rollup3);
    details =
        instanceDao.updateWorkflowInstance(
            summary, overview, null, WorkflowInstance.Status.SUCCEEDED, 12345, null);
    assertFalse(details.isPresent());

    List<WorkflowRollupOverview> result =
        instanceDao.getBatchForeachLatestRunRollupForIterations(
            TEST_WORKFLOW_ID, Arrays.asList(101L, 102L));
    assertEquals(2, result.size());
    assertEquals(20, result.get(0).getTotalLeafCount());
    assertEquals(5, result.get(1).getTotalLeafCount());
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 101);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 102);
  }

  @Test
  public void testGetMinMaxInstanceId() throws Exception {
    List<WorkflowInstance> insertionList = new ArrayList<>();
    try {
      for (int i = 1; i <= 25; i++) {
        WorkflowInstance wfi = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
        wfi.setWorkflowUuid("wfi1-uuid-" + i);
        wfi.setWorkflowInstanceId(100 + i);
        insertionList.add(wfi);
      }
      instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID, insertionList);
      long[] ids = instanceDao.getMinMaxWorkflowInstanceIds(wfi.getWorkflowId());
      assertEquals(125, ids[1]);
      assertEquals(1, ids[0]);
    } finally {
      // delete inserted rows
      for (int i = 1; i <= 25; i++) {
        MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 100 + i);
      }
    }
  }
}
