/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.flow.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.flow.FlowBaseTest;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowGroup;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.models.TaskDef;
import com.netflix.maestro.flow.properties.FlowEngineProperties;
import com.netflix.maestro.flow.runtime.ExecutionPreparer;
import com.netflix.maestro.flow.runtime.FinalFlowStatusCallback;
import com.netflix.maestro.flow.runtime.FlowTask;
import com.netflix.maestro.metrics.MaestroMetrics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ExecutionContextTest extends FlowBaseTest {
  @Mock FlowTask flowTask;
  @Mock private FinalFlowStatusCallback finalCallback;
  @Mock private ExecutionPreparer executionPreparer;
  @Mock private MaestroFlowDao flowDao;
  @Mock private FlowEngineProperties properties;
  @Mock private MaestroMetrics metrics;

  private ExecutionContext context;
  private Flow flow;
  private FlowGroup group;

  @Before
  public void setUp() {
    when(properties.getInternalWorkerNumber()).thenReturn(3);
    context =
        new ExecutionContext(
            Map.of("noop", flowTask),
            finalCallback,
            executionPreparer,
            flowDao,
            properties,
            metrics);
    flow = createFlow();
    group = new FlowGroup(1, 1, "testAddress", 12345);
  }

  @Test
  public void testRun() {
    AtomicInteger counter = new AtomicInteger(0);
    context.run(counter::incrementAndGet);
    context.shutdown();
    assertEquals(1, counter.get());
  }

  @Test
  public void testSchedule() {
    AtomicInteger counter = new AtomicInteger(0);
    context.schedule(counter::incrementAndGet, 1);
    context.shutdown();
    assertEquals(1, counter.get());
  }

  @Test
  public void testPrepareDone() {
    Task prepare = flow.newTask(new TaskDef("prepare", "noop", null, null), true);
    flow.setPrepareTask(prepare);
    assertNull(flow.getPrepareTask().getStartTime());
    prepare.setStatus(Task.Status.COMPLETED);
    prepare.setReasonForIncompletion("hello");
    context.prepare(flow);
    assertNotNull(flow.getPrepareTask().getStartTime());
    verify(flowTask, times(1)).execute(flow, prepare);
    assertEquals("hello", flow.getReasonForIncompletion());
  }

  @Test
  public void testPrepareRetry() {
    Task prepare = flow.newTask(new TaskDef("prepare", "noop", null, null), true);
    flow.setPrepareTask(prepare);
    assertNull(flow.getPrepareTask().getStartTime());

    AssertHelper.assertThrows(
        "should throw and retry",
        MaestroRetryableError.class,
        "prepare task is not done yet",
        () -> context.prepare(flow));
    assertNotNull(flow.getPrepareTask().getStartTime());
    verify(flowTask, times(1)).execute(flow, prepare);
  }

  @Test
  public void testRefresh() {
    Task monitor = flow.newTask(new TaskDef("monitor", "noop", null, null), true);
    flow.setMonitorTask(monitor);

    context.refresh(flow);
    verify(flowTask, times(1)).execute(flow, monitor);
  }

  @Test
  public void testFinalCall() {
    flow.getFlowDef().setFinalFlowStatusCallbackEnabled(false);
    context.finalCall(flow);
    verify(finalCallback, times(0)).onFlowCompleted(flow);
    verify(finalCallback, times(0)).onFlowTerminated(flow);
    verify(finalCallback, times(0)).onFlowFinalized(flow);

    flow.getFlowDef().setFinalFlowStatusCallbackEnabled(true);
    flow.setStatus(Flow.Status.COMPLETED);
    context.finalCall(flow);
    verify(finalCallback, times(1)).onFlowCompleted(flow);
    verify(finalCallback, times(0)).onFlowTerminated(flow);
    verify(finalCallback, times(1)).onFlowFinalized(flow);

    Mockito.reset(finalCallback);
    flow.setStatus(Flow.Status.FAILED);
    context.finalCall(flow);
    verify(finalCallback, times(0)).onFlowCompleted(flow);
    verify(finalCallback, times(1)).onFlowTerminated(flow);
    verify(finalCallback, times(1)).onFlowFinalized(flow);
  }

  @Test
  public void testStart() {
    Task task = flow.newTask(new TaskDef("task", "noop", null, null), false);
    context.start(flow, task);
    verify(flowTask, times(1)).start(flow, task);
  }

  @Test
  public void testExecute() {
    Task task = flow.newTask(new TaskDef("task", "noop", null, null), false);
    context.execute(flow, task);
    verify(flowTask, times(1)).execute(flow, task);
  }

  @Test
  public void testCancel() {
    Task task = flow.newTask(new TaskDef("task", "noop", null, null), false);
    context.cancel(flow, task);
    verify(flowTask, times(1)).cancel(flow, task);
  }

  @Test
  public void testCloneTask() {
    Task task = flow.newTask(new TaskDef("task", "noop", null, null), false);
    context.cloneTask(task);
    verify(executionPreparer, times(1)).cloneTask(task);
  }

  @Test
  public void testCloneTaskException() {
    Task task = flow.newTask(new TaskDef("task", "noop", null, null), false);
    Mockito.doThrow(new RuntimeException("test")).when(executionPreparer).cloneTask(task);
    AssertHelper.assertThrows(
        "should throw",
        MaestroUnprocessableEntityException.class,
        "cannot clone task: [task]",
        () -> context.cloneTask(task));
  }

  @Test
  public void testSaveFlow() {
    context.saveFlow(flow);
    verify(flowDao, times(1)).insertFlow(flow);
  }

  @Test
  public void testSaveFlowRetry() {
    Mockito.doThrow(new MaestroInternalError("test")).when(flowDao).insertFlow(flow);
    AssertHelper.assertThrows(
        "should throw and retry",
        MaestroRetryableError.class,
        "insertFlow is failed for test-flow-ref and please retry",
        () -> context.saveFlow(flow));
  }

  @Test
  public void testDeleteFlow() {
    context.deleteFlow(flow);
    verify(flowDao, times(1)).deleteFlow(flow);
  }

  @Test
  public void testGetFlowsFrom() {
    when(flowDao.getFlows(group, 10, "")).thenReturn(List.of());
    assertEquals(List.of(), context.getFlowsFrom(group, 10, ""));
    verify(flowDao, times(1)).getFlows(group, 10, "");
  }

  @Test
  public void testGetFlowsFromWithError() {
    Mockito.doThrow(new MaestroInternalError("test")).when(flowDao).getFlows(group, 10, "");
    assertNull(context.getFlowsFrom(group, 10, ""));
    verify(flowDao, times(1)).getFlows(group, 10, "");
  }

  @Test
  public void testResumeFlowWithPrepare() {
    Task prepare = flow.newTask(new TaskDef("prepare", "noop", null, null), true);
    flow.setPrepareTask(prepare);
    prepare.setStatus(Task.Status.COMPLETED);
    when(executionPreparer.resume(flow)).thenReturn(true);

    context.resumeFlow(flow);
    verify(executionPreparer, times(1)).resume(flow);
    verify(flowTask, times(1)).execute(flow, prepare);
  }

  @Test
  public void testResumeFlowWithoutPrepare() {
    when(executionPreparer.resume(flow)).thenReturn(false);

    context.resumeFlow(flow);
    verify(executionPreparer, times(1)).resume(flow);
    verify(flowTask, times(0)).execute(any(), any());
  }

  @Test
  public void testResumeFlowWithException() {
    Mockito.doThrow(new RuntimeException("test")).when(executionPreparer).resume(flow);
    AssertHelper.assertThrows(
        "should throw and retry",
        MaestroRetryableError.class,
        "retry resuming flow due to an exception",
        () -> context.resumeFlow(flow));
  }

  @Test
  public void testTrySaveGroup() {
    context.trySaveGroup(group.groupId(), group.address());
    verify(flowDao, times(1)).insertGroup(group.groupId(), group.address());
  }

  @Test
  public void testTrySaveGroupWithShutdown() {
    context.shutdown();
    AssertHelper.assertThrows(
        "should throw during shutdown",
        MaestroRetryableError.class,
        "ExecutionContext is shutdown and cannot save a group-[1] and please retry",
        () -> context.trySaveGroup(group.groupId(), group.address()));
  }

  @Test
  public void testTrySaveGroupWithException() {
    Mockito.doThrow(new MaestroInternalError("test"))
        .when(flowDao)
        .insertGroup(group.groupId(), group.address());
    AssertHelper.assertThrows(
        "should throw and retry",
        MaestroRetryableError.class,
        "insertGroup is failed for group-[1] and please retry",
        () -> context.trySaveGroup(group.groupId(), group.address()));
  }

  @Test
  public void testClaimGroup() {
    context.claimGroup();
    verify(flowDao, times(1)).claimExpiredGroup(any(), anyLong());
  }

  @Test
  public void testHeartbeatGroup() {
    when(flowDao.heartbeatGroup(group)).thenReturn(12345L);
    when(properties.getExpirationDurationInMillis()).thenReturn(20000L);
    var validUntil = context.heartbeatGroup(group);

    verify(flowDao, times(1)).heartbeatGroup(group);
    assertEquals(32345, validUntil);
  }

  @Test
  public void testReleaseGroup() {
    context.releaseGroup(group);
    verify(flowDao, times(1)).releaseGroup(group);
  }
}
