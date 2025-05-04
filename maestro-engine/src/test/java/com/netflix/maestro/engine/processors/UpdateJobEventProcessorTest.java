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
package com.netflix.maestro.engine.processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.concurrency.TagPermitManager;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.NotificationJobEvent;
import com.netflix.maestro.queue.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowVersionUpdateJobEvent;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class UpdateJobEventProcessorTest extends MaestroEngineBaseTest {

  @Mock private TagPermitManager tagPermitManager;
  @Mock private MaestroStepInstanceActionDao actionDao;
  @Mock private InstanceStepConcurrencyHandler handler;
  @Mock private FlowOperation flowOperation;
  @Mock private MaestroQueueSystem queueSystem;

  @Mock private StepInstanceUpdateJobEvent jobEvent1;
  @Mock private WorkflowInstanceUpdateJobEvent jobEvent2;
  @Mock private WorkflowInstanceUpdateJobEvent.WorkflowInstanceChangeRecord changeRecord2;
  @Mock private WorkflowInstanceUpdateJobEvent.WorkflowInstanceChangeRecord changeRecord3;
  @Mock private WorkflowVersionUpdateJobEvent jobEvent3;

  private final String workflowId = "sample-minimal-wf";
  private UpdateJobEventProcessor processor;

  @Before
  public void before() throws Exception {
    processor =
        new UpdateJobEventProcessor(
            tagPermitManager, handler, actionDao, flowOperation, queueSystem);
    when(jobEvent1.getType()).thenCallRealMethod();
    when(jobEvent2.getType()).thenCallRealMethod();
    when(jobEvent3.getType()).thenCallRealMethod();
    when(jobEvent3.getWorkflowId()).thenReturn("foo");
  }

  @Test
  public void testConsumeStepInstanceUpdateJobEvent() {
    var res = processor.process(jobEvent1);
    verify(jobEvent1, times(1)).getType();
    verify(jobEvent1, times(1)).hasTerminal();
    assertTrue(res.isPresent());
    assertEquals(jobEvent1, ((NotificationJobEvent) res.get()).getEvent());
  }

  @Test
  public void testNotReleaseTagPermitOrCleanUpStepActionIfRunning() {
    when(jobEvent1.getPendingRecords())
        .thenReturn(
            Collections.singletonList(
                StepInstanceUpdateJobEvent.createRecord(
                    StepInstance.Status.RUNNING, StepInstance.Status.RUNNING, 12345L)));
    var res = processor.process(jobEvent1);
    assertTrue(res.isPresent());
    assertEquals(jobEvent1, ((NotificationJobEvent) res.get()).getEvent());
    verify(tagPermitManager, times(0)).releaseTagPermits(anyString());
    verify(actionDao, times(0)).cleanUp(any());
    verify(jobEvent1, times(1)).getType();
  }

  @Test
  public void testReleaseTagPermitAndCleanUpStepActionIfTerminal() {
    when(jobEvent1.hasTerminal()).thenReturn(true);
    var res = processor.process(jobEvent1);
    assertTrue(res.isPresent());
    assertEquals(jobEvent1, ((NotificationJobEvent) res.get()).getEvent());
    verify(tagPermitManager, times(1)).releaseTagPermits(jobEvent1.getStepUuid());
    verify(actionDao, times(1)).cleanUp(jobEvent1);
    verify(jobEvent1, times(1)).getType();
  }

  @Test
  public void testReleaseInstanceStepConcurrency() {
    // check unregistering step
    when(jobEvent1.hasTerminal()).thenReturn(true);
    var res = processor.process(jobEvent1);
    assertTrue(res.isPresent());
    assertEquals(jobEvent1, ((NotificationJobEvent) res.get()).getEvent());
    verify(handler, times(1)).unregisterStep(jobEvent1.getCorrelationId(), jobEvent1.getStepUuid());

    // check instance cleanup
    when(jobEvent2.getChangeRecords()).thenReturn(Collections.singletonList(changeRecord2));
    when(changeRecord2.getNewStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    when(jobEvent2.getWorkflowId()).thenReturn(workflowId);
    when(changeRecord2.getParent()).thenReturn(new UpstreamInitiator.Info());

    res = processor.process(jobEvent2);
    assertTrue(res.isPresent());
    assertEquals(jobEvent2, ((NotificationJobEvent) res.get()).getEvent());
    verify(handler, times(1)).cleanUp(changeRecord2.getCorrelationId());
    verify(handler, times(0)).removeInstance(any(), anyInt(), any());
    verify(queueSystem, times(1)).enqueueOrThrow(any());

    when(changeRecord2.getDepth()).thenReturn(2);
    res = processor.process(jobEvent2);
    assertTrue(res.isPresent());
    assertEquals(jobEvent2, ((NotificationJobEvent) res.get()).getEvent());
    verify(handler, times(1)).cleanUp(changeRecord2.getCorrelationId());
    verify(handler, times(1))
        .removeInstance(
            changeRecord2.getCorrelationId(),
            changeRecord2.getDepth(),
            changeRecord2.getWorkflowUuid());
    verify(flowOperation, times(1)).wakeUp(anyLong(), anyString(), any());
    verify(queueSystem, times(2)).enqueueOrThrow(any());
  }

  @Test
  public void testConsumeWorkflowInstanceUpdateJobEvent() {
    when(jobEvent2.getChangeRecords()).thenReturn(Collections.singletonList(changeRecord2));
    when(changeRecord2.getNewStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    var res = processor.process(jobEvent2);
    assertTrue(res.isPresent());
    assertEquals(jobEvent2, ((NotificationJobEvent) res.get()).getEvent());
    verify(jobEvent2, times(1)).getType();
    verify(jobEvent2, times(0)).getWorkflowId();
  }

  @Test
  public void testConsumeWorkflowInstanceUpdateJobEventWithStartWorkflowJobEvent() {
    when(jobEvent2.getChangeRecords()).thenReturn(List.of(changeRecord2, changeRecord3));
    when(changeRecord2.getNewStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    when(changeRecord3.getNewStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(jobEvent2.getWorkflowId()).thenReturn(workflowId);
    var res = processor.process(jobEvent2);
    assertTrue(res.isPresent());
    assertEquals(jobEvent2, ((NotificationJobEvent) res.get()).getEvent());
    verify(jobEvent2, times(1)).getType();
    verify(jobEvent2, times(1)).getType();
    verify(jobEvent2, times(2)).getWorkflowId();
    verify(actionDao, times(1)).cleanUp(eq(workflowId), anyLong(), anyLong());
    verify(handler, times(1)).cleanUp(any());
    verify(queueSystem, times(1)).enqueueOrThrow(any());
  }

  @Test
  public void testConsumeWorkflowInstanceUpdateJobEventWithEnqueueError() {
    when(jobEvent2.getChangeRecords()).thenReturn(List.of(changeRecord2));
    when(changeRecord2.getNewStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    when(jobEvent2.getWorkflowId()).thenReturn(workflowId);
    doThrow(new RuntimeException("test")).when(queueSystem).enqueueOrThrow(any());
    AssertHelper.assertThrows(
        "Failed publishing", RuntimeException.class, "test", () -> processor.process(jobEvent2));
    verify(jobEvent2, times(1)).getType();
    verify(jobEvent2, times(2)).getWorkflowId();
    verify(actionDao, times(1)).cleanUp(eq(workflowId), anyLong(), anyLong());
    verify(queueSystem, times(1)).enqueueOrThrow(any());
  }

  @Test
  public void testConsumeWorkflowVersionJobEvent() {
    when(jobEvent3.getWorkflowId()).thenReturn(workflowId);
    var res = processor.process(jobEvent3);
    assertTrue(res.isPresent());
    assertEquals(jobEvent3, ((NotificationJobEvent) res.get()).getEvent());
    verify(queueSystem, times(1)).enqueueOrThrow(any());
  }
}
