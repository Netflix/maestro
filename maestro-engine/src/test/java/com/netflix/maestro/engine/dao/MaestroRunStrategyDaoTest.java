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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.jobevents.RunWorkflowInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.TerminateThenRunInstanceJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.publisher.NoOpMaestroJobEventPublisher;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.instance.RunConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MaestroRunStrategyDaoTest extends MaestroDaoBaseTest {
  private static final String TEST_WORKFLOW_ID = "sample-dag-test-3";
  private static final String TEST_WORKFLOW_INSTANCE =
      "fixtures/instances/sample-workflow-instance-created.json";

  private MaestroWorkflowInstanceDao dao;
  private MaestroRunStrategyDao runStrategyDao;
  private WorkflowInstance wfi;
  private final MaestroJobEventPublisher publisher = mock(NoOpMaestroJobEventPublisher.class);

  @Before
  public void setUp() throws Exception {
    MaestroWorkflowDao workflowDao =
        new MaestroWorkflowDao(
            dataSource,
            MAPPER,
            config,
            publisher,
            mock(TriggerSubscriptionClient.class),
            metricRepo);
    WorkflowDefinition definition =
        loadObject(
            "fixtures/workflows/definition/sample-minimal-wf.json", WorkflowDefinition.class);
    definition.setWorkflow(definition.getWorkflow().toBuilder().id(TEST_WORKFLOW_ID).build());
    Properties properties = new Properties();
    properties.setOwner(User.builder().name("tester").build());
    workflowDao.addWorkflowDefinition(definition, properties);
    verify(publisher, times(1)).publishOrThrow(any(WorkflowVersionUpdateJobEvent.class), any());
    reset(publisher);

    dao = new MaestroWorkflowInstanceDao(dataSource, MAPPER, config, publisher, metricRepo);
    runStrategyDao = new MaestroRunStrategyDao(dataSource, MAPPER, config, publisher, metricRepo);
    wfi = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi.setWorkflowInstanceId(0L);
    wfi.setWorkflowRunId(0L);
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    assertEquals(TEST_WORKFLOW_ID, wfi.getWorkflowId());
    assertEquals(1, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("8a0bd56f-745f-4a2c-b81b-1b2f89127e73", wfi.getWorkflowUuid());
    verifyPublish(1, 0, 0, 0, 0);
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(1, latestRun.getWorkflowInstanceId());
    assertEquals(wfi.getWorkflowUuid(), latestRun.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.CREATED, latestRun.getStatus());
    reset(publisher);
  }

  private void verifyPublish(int start, int delayStart, int run, int terminate, int update) {
    verify(publisher, times(start)).publishOrThrow(any(StartWorkflowJobEvent.class), any());
    verify(publisher, times(delayStart))
        .publishOrThrow(any(StartWorkflowJobEvent.class), anyLong(), any());
    verify(publisher, times(run)).publishOrThrow(any(RunWorkflowInstancesJobEvent.class), any());
    verify(publisher, times(terminate))
        .publishOrThrow(any(TerminateThenRunInstanceJobEvent.class), any());
    verify(publisher, times(update))
        .publishOrThrow(any(WorkflowInstanceUpdateJobEvent.class), any());
    verify(publisher, times(start + run + terminate + update)).publishOrThrow(any(), any());
    reset(publisher);
  }

  @After
  public void tearDown() {
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 1);
    AssertHelper.assertThrows(
        "cannot get non-existing workflow instance",
        MaestroNotFoundException.class,
        "workflow instance [sample-dag-test-3][1][0] not found (either not created or deleted)",
        () -> dao.getLatestWorkflowInstanceRun(TEST_WORKFLOW_ID, 1));
  }

  @Test
  public void testStartWithRunStrategyForNewStart() {
    wfi.setWorkflowInstanceId(0L);
    wfi.setWorkflowRunId(0L);
    wfi.setWorkflowUuid("test-uuid");
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    assertEquals(2, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid", latestRun.getWorkflowUuid());
    verifyPublish(1, 0, 0, 0, 0);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testStartWithRunStrategyForDeletedWorkflow() {
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID);
    wfi.setWorkflowInstanceId(0L);
    wfi.setWorkflowRunId(0L);
    wfi.setWorkflowUuid("test-uuid");
    AssertHelper.assertThrows(
        "cannot start a deleted workflow",
        MaestroNotFoundException.class,
        "Cannot find workflow [sample-dag-test-3]",
        () -> runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY));
  }

  @Test
  public void testStartWithRunStrategyForStartWithRunId() {
    wfi.setWorkflowInstanceId(0L);
    wfi.setWorkflowRunId(2L);
    wfi.setWorkflowUuid("test-uuid");
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    assertEquals(2, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId()); // reset to 1
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid", latestRun.getWorkflowUuid());
    verifyPublish(1, 0, 0, 0, 0);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testStartWithRunStrategyForRestart() {
    dao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.FAILED, "kill the test");
    verifyPublish(0, 0, 0, 0, 1);
    wfi.setWorkflowRunId(0L);
    wfi.setWorkflowUuid("test-uuid");
    wfi.setRunConfig(new RunConfig());
    wfi.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    assertEquals(1, wfi.getWorkflowInstanceId());
    assertEquals(2, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(1, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid", latestRun.getWorkflowUuid());
    verifyPublish(1, 0, 0, 0, 0);
  }

  @Test
  public void testStartWithRunStrategyForInvalidRestart() {
    wfi.setWorkflowRunId(0L);
    wfi.setWorkflowUuid("test-uuid");
    wfi.setRunConfig(new RunConfig());
    wfi.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    AssertHelper.assertThrows(
        "Invalid restart when there is a non-terminal run for this instance id",
        MaestroInvalidStatusException.class,
        "There is already a workflow instance run [sample-dag-test-3][1][1] and cannot restart another run",
        () -> runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY));
  }

  @Test
  public void testStartWithRunStrategyToUpdateAncestorStatus() {
    dao.tryTerminateQueuedInstance(wfi, WorkflowInstance.Status.FAILED, "test");
    WorkflowInstance.Status status =
        dao.getWorkflowInstanceStatus(wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), 1L);
    assertEquals(WorkflowInstance.Status.FAILED, status);
    String rawStatus =
        dao.getWorkflowInstanceRawStatus(wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), 1L);
    assertEquals("FAILED", rawStatus);

    wfi.setWorkflowRunId(0L);
    wfi.setWorkflowUuid("test-uuid");
    wfi.setRunConfig(new RunConfig());
    wfi.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);

    status = dao.getWorkflowInstanceStatus(wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), 1L);
    assertEquals(WorkflowInstance.Status.FAILED, status);
    rawStatus =
        dao.getWorkflowInstanceRawStatus(wfi.getWorkflowId(), wfi.getWorkflowInstanceId(), 1L);
    assertEquals("FAILED_2", rawStatus);
  }

  @Test
  public void testStartWorkflowInstanceWithSameUuid() {
    wfi.setWorkflowUuid("8a0bd56f-745f-4a2c-b81b-1b2f89127e73");
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(0, res);
    assertEquals(1, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("8a0bd56f-745f-4a2c-b81b-1b2f89127e73", wfi.getWorkflowUuid());
    verifyPublish(0, 0, 0, 0, 0);
  }

  @Test
  public void testStartRunStrategyWithQueue() {
    wfi.setWorkflowInstanceId(0L);
    wfi.setWorkflowRunId(0L);
    wfi.setWorkflowUuid("test-uuid");
    int res = runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("PARALLEL"));
    assertEquals(1, res);
    assertEquals(2, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid", latestRun.getWorkflowUuid());
    verifyPublish(1, 0, 0, 0, 0);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testStartRunStrategyWithFirstOnly() {
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 1);
    wfi.setWorkflowUuid("test-uuid");
    wfi.setWorkflowInstanceId(0);
    int res = runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("FIRST_ONLY"));
    assertEquals(1, res);
    assertEquals(1, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(1, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid", latestRun.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.CREATED, latestRun.getStatus());
    verifyPublish(0, 0, 0, 1, 0);

    wfi.setWorkflowInstanceId(0);
    res = runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("FIRST_ONLY"));
    assertEquals(0, res);
    assertEquals(0, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    verifyPublish(0, 0, 0, 0, 0);

    wfi.setWorkflowUuid("test-uuid-1");
    wfi.setWorkflowInstanceId(0);
    res = runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("FIRST_ONLY"));
    assertEquals(-1, res);
    assertEquals(2, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid-1", wfi.getWorkflowUuid());
    latestRun = dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid-1", latestRun.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.STOPPED, latestRun.getStatus());
    verifyPublish(0, 0, 0, 0, 1);

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testStartRunStrategyWithLastOnly() {
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 1);
    wfi.setWorkflowUuid("test-uuid");
    wfi.setWorkflowInstanceId(0);
    int res = runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("LAST_ONLY"));
    assertEquals(1, res);
    assertEquals(1, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(1, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid", latestRun.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.CREATED, latestRun.getStatus());
    verifyPublish(0, 0, 0, 1, 0);

    wfi.setWorkflowInstanceId(0);
    res = runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("LAST_ONLY"));
    assertEquals(0, res);
    assertEquals(0, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    verifyPublish(0, 0, 0, 0, 0);

    wfi.setWorkflowUuid("test-uuid-1");
    wfi.setWorkflowInstanceId(0);
    res = runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("LAST_ONLY"));
    assertEquals(1, res);
    assertEquals(2, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid-1", wfi.getWorkflowUuid());

    WorkflowInstance previous = dao.getWorkflowInstanceRun(wfi.getWorkflowId(), 1, 1);
    assertEquals(1, previous.getWorkflowInstanceId());
    assertEquals("test-uuid", previous.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.STOPPED, previous.getStatus());
    latestRun = dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid-1", latestRun.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.CREATED, latestRun.getStatus());
    verifyPublish(0, 0, 0, 1, 1);

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testInvalidState() {
    wfi.setWorkflowInstanceId(0L);
    wfi.setWorkflowRunId(0L);
    wfi.setWorkflowUuid("test-uuid");
    int res = runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("PARALLEL"));
    assertEquals(1, res);
    assertEquals(2, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid", latestRun.getWorkflowUuid());
    verifyPublish(1, 0, 0, 0, 0);

    wfi.setWorkflowUuid("test-uuid-1");
    wfi.setWorkflowInstanceId(0);
    wfi.setWorkflowRunId(0L);
    AssertHelper.assertThrows(
        "Invalid state when switching run strategies",
        IllegalArgumentException.class,
        "finding more than 1 non-terminal runs beside [InstanceRunUuid",
        () -> runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("FIRST_ONLY")));

    wfi.setWorkflowUuid("test-uuid-1");
    wfi.setWorkflowInstanceId(0);
    wfi.setWorkflowRunId(0L);
    AssertHelper.assertThrows(
        "Invalid state when switching run strategies",
        IllegalArgumentException.class,
        "finding more than 1 pending runs beside [sample-dag-test-3][2] with LAST_ONLY run strategy",
        () -> runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("LAST_ONLY")));

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  private List<WorkflowInstance> prepareBatch() throws Exception {
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 1);
    WorkflowInstance wfi1 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi1.setWorkflowUuid("wfi1-uuid");
    wfi1.setWorkflowInstanceId(0);
    WorkflowInstance wfi2 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi2.setWorkflowUuid("wfi1-uuid");
    wfi2.setWorkflowInstanceId(0);
    WorkflowInstance wfi3 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
    wfi3.setWorkflowUuid("wfi3-uuid");
    wfi3.setWorkflowInstanceId(0);
    return Arrays.asList(wfi1, wfi2, wfi3);
  }

  @Test
  public void testStartBatchRunStrategyWithQueue() throws Exception {
    List<WorkflowInstance> batch = prepareBatch();
    int[] res =
        runStrategyDao.startBatchWithRunStrategy(
            TEST_WORKFLOW_ID, RunStrategy.create("PARALLEL"), batch);
    assertArrayEquals(new int[] {1, 0, 1}, res);
    assertEquals(1, batch.get(0).getWorkflowInstanceId());
    assertEquals(0, batch.get(1).getWorkflowInstanceId());
    assertEquals(2, batch.get(2).getWorkflowInstanceId());

    WorkflowInstance previous = dao.getWorkflowInstanceRun(TEST_WORKFLOW_ID, 1, 1);
    WorkflowInstance latestRun = dao.getLatestWorkflowInstanceRun(TEST_WORKFLOW_ID, 2);
    assertEquals(1, previous.getWorkflowInstanceId());
    assertEquals("wfi1-uuid", previous.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.CREATED, previous.getStatus());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("wfi3-uuid", latestRun.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.CREATED, latestRun.getStatus());
    verifyPublish(1, 0, 0, 0, 0);

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testStartBatchRunStrategyWithFirstOnly() throws Exception {
    List<WorkflowInstance> batch = prepareBatch();
    int[] res =
        runStrategyDao.startBatchWithRunStrategy(
            TEST_WORKFLOW_ID, RunStrategy.create("FIRST_ONLY"), batch);
    assertArrayEquals(new int[] {1, 0, -1}, res);
    assertEquals(1, batch.get(0).getWorkflowInstanceId());
    assertEquals(0, batch.get(1).getWorkflowInstanceId());
    assertEquals(2, batch.get(2).getWorkflowInstanceId());

    WorkflowInstance previous = dao.getWorkflowInstanceRun(TEST_WORKFLOW_ID, 1, 1);
    WorkflowInstance latestRun = dao.getLatestWorkflowInstanceRun(TEST_WORKFLOW_ID, 2);
    assertEquals(1, previous.getWorkflowInstanceId());
    assertEquals("wfi1-uuid", previous.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.CREATED, previous.getStatus());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("wfi3-uuid", latestRun.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.STOPPED, latestRun.getStatus());
    verifyPublish(0, 0, 0, 1, 1);

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testStartBatchRunStrategyWithLastOnly() throws Exception {
    List<WorkflowInstance> batch = prepareBatch();
    int[] res =
        runStrategyDao.startBatchWithRunStrategy(
            TEST_WORKFLOW_ID, RunStrategy.create("LAST_ONLY"), batch);
    assertArrayEquals(new int[] {-1, 0, 1}, res);
    assertEquals(1, batch.get(0).getWorkflowInstanceId());
    assertEquals(0, batch.get(1).getWorkflowInstanceId());
    assertEquals(2, batch.get(2).getWorkflowInstanceId());

    WorkflowInstance previous = dao.getWorkflowInstanceRun(TEST_WORKFLOW_ID, 1, 1);
    WorkflowInstance latestRun = dao.getLatestWorkflowInstanceRun(TEST_WORKFLOW_ID, 2);
    assertEquals(1, previous.getWorkflowInstanceId());
    assertEquals("wfi1-uuid", previous.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.STOPPED, previous.getStatus());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("wfi3-uuid", latestRun.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.CREATED, latestRun.getStatus());
    verifyPublish(0, 0, 0, 1, 1);

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testDequeueWithRunStrategy() {
    wfi.setWorkflowInstanceId(0L);
    wfi.setWorkflowRunId(0L);
    wfi.setWorkflowUuid("test-uuid");
    int res = runStrategyDao.startWithRunStrategy(wfi, RunStrategy.create("PARALLEL"));
    assertEquals(1, res);
    assertEquals(2, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid", latestRun.getWorkflowUuid());
    verifyPublish(1, 0, 0, 0, 0);

    res = runStrategyDao.dequeueWithRunStrategy(TEST_WORKFLOW_ID, RunStrategy.create("SEQUENTIAL"));
    assertEquals(1, res);
    res = runStrategyDao.dequeueWithRunStrategy(TEST_WORKFLOW_ID, RunStrategy.create("PARALLEL"));
    assertEquals(2, res);
    res =
        runStrategyDao.dequeueWithRunStrategy(
            TEST_WORKFLOW_ID, RunStrategy.create("STRICT_SEQUENTIAL"));
    assertEquals(1, res);

    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(TEST_WORKFLOW_ID);
    summary.setWorkflowInstanceId(1L);
    summary.setWorkflowRunId(1L);
    dao.updateWorkflowInstance(summary, null, null, WorkflowInstance.Status.FAILED, 123L);
    res =
        runStrategyDao.dequeueWithRunStrategy(
            TEST_WORKFLOW_ID, RunStrategy.create("STRICT_SEQUENTIAL"));
    assertEquals(0, res);

    res = runStrategyDao.dequeueWithRunStrategy(TEST_WORKFLOW_ID, RunStrategy.create("FIRST_ONLY"));
    assertEquals(0, res);
    res = runStrategyDao.dequeueWithRunStrategy(TEST_WORKFLOW_ID, RunStrategy.create("LAST_ONLY"));
    assertEquals(0, res);

    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }

  @Test
  public void testDequeueWithSizeLimit() throws Exception {
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 1);
    RunStrategy runStrategy = RunStrategy.create(10);
    for (int i = 0; i < Constants.DEQUEUE_SIZE_LIMIT + 1; ++i) {
      WorkflowInstance wfi1 = loadObject(TEST_WORKFLOW_INSTANCE, WorkflowInstance.class);
      wfi1.setWorkflowUuid("wfi-uuid-" + i);
      wfi1.setWorkflowInstanceId(0);
      int res = runStrategyDao.startWithRunStrategy(wfi1, RunStrategy.create(10));
      WorkflowInstance latestRun = dao.getLatestWorkflowInstanceRun(TEST_WORKFLOW_ID, i + 1);
      assertEquals(1, res);
      assertEquals(i + 1, latestRun.getWorkflowInstanceId());
      assertEquals(1, latestRun.getWorkflowRunId());
      assertEquals("wfi-uuid-" + i, latestRun.getWorkflowUuid());

      res = runStrategyDao.dequeueWithRunStrategy(TEST_WORKFLOW_ID, runStrategy);
      if (i < runStrategy.getWorkflowConcurrency()) {
        assertEquals(i + 1, res);
        verifyPublish(1, 0, i + 1, 0, 0);
      } else {
        assertEquals(runStrategy.getWorkflowConcurrency(), res);
        verifyPublish(1, 0, (int) runStrategy.getWorkflowConcurrency(), 0, 0);
      }
    }

    runStrategy = RunStrategy.create(Constants.DEQUEUE_SIZE_LIMIT - 1);
    int res = runStrategyDao.dequeueWithRunStrategy(TEST_WORKFLOW_ID, runStrategy);
    assertEquals(runStrategy.getWorkflowConcurrency(), res);
    verify(publisher, times(0)).publishOrThrow(any(StartWorkflowJobEvent.class), anyLong(), any());
    verifyPublish(0, 0, (int) runStrategy.getWorkflowConcurrency(), 0, 0);

    runStrategy = RunStrategy.create(0);
    res = runStrategyDao.dequeueWithRunStrategy(TEST_WORKFLOW_ID, runStrategy);
    assertEquals(0, res);
    verifyPublish(0, 0, 0, 0, 0);

    runStrategy = RunStrategy.create(Constants.DEQUEUE_SIZE_LIMIT + 1);
    res = runStrategyDao.dequeueWithRunStrategy(TEST_WORKFLOW_ID, runStrategy);
    assertEquals(Constants.DEQUEUE_SIZE_LIMIT, res);
    verifyPublish(0, 1, Constants.DEQUEUE_SIZE_LIMIT, 0, 0);

    for (int i = Constants.DEQUEUE_SIZE_LIMIT; i >= 1; --i) {
      MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, i + 1);
    }
  }

  @Test
  public void testPersistFailedRun() {
    wfi.setWorkflowInstanceId(0L);
    wfi.setWorkflowRunId(0L);
    wfi.setWorkflowUuid("test-uuid");
    wfi.setStatus(WorkflowInstance.Status.FAILED);
    AssertHelper.assertThrows(
        "Cannot add a failed instance with a null timeline",
        NullPointerException.class,
        "workflow instance timeline cannot be null",
        () -> runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY));
    wfi.setTimeline(new Timeline(Collections.singletonList(TimelineLogEvent.info("test"))));
    int res = runStrategyDao.startWithRunStrategy(wfi, Defaults.DEFAULT_RUN_STRATEGY);
    assertEquals(1, res);
    assertEquals(2, wfi.getWorkflowInstanceId());
    assertEquals(1, wfi.getWorkflowRunId());
    assertEquals("test-uuid", wfi.getWorkflowUuid());
    WorkflowInstance latestRun =
        dao.getLatestWorkflowInstanceRun(wfi.getWorkflowId(), wfi.getWorkflowInstanceId());
    assertEquals(2, latestRun.getWorkflowInstanceId());
    assertEquals("test-uuid", latestRun.getWorkflowUuid());
    assertEquals(WorkflowInstance.Status.FAILED, latestRun.getStatus());
    assertEquals("test", latestRun.getTimeline().getTimelineEvents().get(0).getMessage());
    verifyPublish(0, 0, 0, 0, 1);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 2);
  }
}
