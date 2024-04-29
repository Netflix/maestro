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
package com.netflix.maestro.engine.messageprocessors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.concurrency.TagPermitManager;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.engine.processors.PublishJobEventProcessor;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import com.netflix.maestro.models.events.WorkflowDefinitionChangeEvent;
import com.netflix.maestro.models.events.WorkflowInstanceStatusChangeEvent;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class PublishJobEventProcessorTest extends MaestroEngineBaseTest {

  @Mock private MaestroJobEventPublisher publisher;
  @Mock private MaestroNotificationPublisher eventClient;
  @Mock private TagPermitManager tagPermitManager;
  @Mock private MaestroStepInstanceActionDao actionDao;

  @Mock private InstanceStepConcurrencyHandler handler;

  @Mock private StepInstanceUpdateJobEvent jobEvent1;
  @Mock private StepInstanceStatusChangeEvent changeEvent1;
  @Mock private WorkflowInstanceUpdateJobEvent jobEvent2;
  @Mock private WorkflowInstanceStatusChangeEvent changeEvent2;
  @Mock private WorkflowInstanceStatusChangeEvent changeEvent3;
  @Mock private WorkflowVersionUpdateJobEvent jobEvent3;

  private final String clusterName = "test-cluster";
  private final String workflowId = "sample-minimal-wf";
  private PublishJobEventProcessor processor;

  @Before
  public void before() throws Exception {
    processor =
        new PublishJobEventProcessor(
            new WorkflowHelper(null, null, null, null, publisher),
            eventClient,
            tagPermitManager,
            actionDao,
            handler,
            clusterName);
    when(jobEvent1.getType()).thenCallRealMethod();
    when(jobEvent2.getType()).thenCallRealMethod();
    when(jobEvent3.getType()).thenCallRealMethod();
    when(jobEvent3.getWorkflowId()).thenReturn("foo");
  }

  @Test
  public void testConsumeStepInstanceUpdateJobEvent() {
    when(jobEvent1.toMaestroEvent(clusterName)).thenReturn(changeEvent1);
    processor.process(() -> jobEvent1);
    verify(jobEvent1, times(1)).getType();
    verify(eventClient, times(1)).send(changeEvent1);
  }

  @Test
  public void testConsumeWorkflowInstanceUpdateJobEvent() {
    when(changeEvent2.getNewStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(jobEvent2.toMaestroEventStream(clusterName)).thenReturn(Stream.of(changeEvent2));
    processor.process(() -> jobEvent2);
    verify(jobEvent2, times(1)).getType();
    verify(jobEvent2, times(0)).getWorkflowId();
    verify(eventClient, times(1)).send(changeEvent2);
  }

  @Test
  public void testConsumeWorkflowInstanceUpdateJobEventWithStartWorkflowJobEvent() {
    when(changeEvent2.getNewStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    when(changeEvent3.getNewStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(jobEvent2.getWorkflowId()).thenReturn(workflowId);
    when(jobEvent2.toMaestroEventStream(clusterName))
        .thenReturn(Stream.of(changeEvent2, changeEvent3));
    processor.process(() -> jobEvent2);
    verify(jobEvent2, times(1)).getType();
    verify(jobEvent2, times(2)).getWorkflowId();
    verify(actionDao, times(1)).cleanUp(eq(workflowId), anyLong(), anyLong());
    verify(publisher, times(1)).publishOrThrow(any(StartWorkflowJobEvent.class));
    verify(eventClient, times(1)).send(changeEvent2);
    verify(eventClient, times(1)).send(changeEvent3);
  }

  @Test
  public void testConsumeWorkflowInstanceUpdateJobEventWithPublisherThrow() {
    when(changeEvent2.getNewStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    when(jobEvent2.getWorkflowId()).thenReturn(workflowId);
    when(jobEvent2.toMaestroEventStream(clusterName)).thenReturn(Stream.of(changeEvent2));
    doCallRealMethod().when(publisher).publishOrThrow(any());
    doCallRealMethod().when(publisher).publishOrThrow(any(), any());
    doCallRealMethod().when(publisher).publishOrThrow(any(), anyLong(), any());
    when(publisher.publish(any(), anyLong())).thenReturn(Optional.of(Details.create("test")));
    AssertHelper.assertThrows(
        "Failed publishing",
        MaestroRetryableError.class,
        "Failed sending StartWorkflowJobEvent(workflowId=sample-minimal-wf), retry it",
        () -> processor.process(() -> jobEvent2));
    verify(jobEvent2, times(1)).getType();
    verify(jobEvent2, times(2)).getWorkflowId();
    verify(actionDao, times(1)).cleanUp(eq(workflowId), anyLong(), anyLong());
    verify(publisher, times(1)).publishOrThrow(any());
  }

  @Test
  public void testConsumeWorkflowVersionJobEvent() {
    when(jobEvent3.toMaestroEvent(clusterName))
        .thenReturn(WorkflowDefinitionChangeEvent.builder().build());
    processor.process(() -> jobEvent3);
    verify(publisher, times(1)).publishOrThrow(any());
    verify(eventClient, times(1)).send(any());
  }

  @Test
  public void testNotReleaseTagPermitOrCleanUpStepActionIfRunning() {
    when(jobEvent1.toMaestroEvent(clusterName)).thenReturn(changeEvent1);
    when(jobEvent1.getPendingRecords())
        .thenReturn(
            Collections.singletonList(
                StepInstanceUpdateJobEvent.createRecord(
                    StepInstance.Status.RUNNING, StepInstance.Status.RUNNING, 12345L)));
    processor.process(() -> jobEvent1);
    verify(tagPermitManager, times(0)).releaseTagPermits(anyString());
    verify(actionDao, times(0)).cleanUp(any());
    verify(jobEvent1, times(1)).getType();
    verify(eventClient, times(1)).send(changeEvent1);
  }

  @Test
  public void testReleaseTagPermitAndCleanUpStepActionIfTerminal() {
    when(jobEvent1.toMaestroEvent(clusterName)).thenReturn(changeEvent1);
    when(jobEvent1.hasTerminal()).thenReturn(true);
    processor.process(() -> jobEvent1);
    verify(tagPermitManager, times(1)).releaseTagPermits(jobEvent1.getStepUuid());
    verify(actionDao, times(1)).cleanUp(jobEvent1);
    verify(jobEvent1, times(1)).getType();
    verify(eventClient, times(1)).send(changeEvent1);
  }

  @Test
  public void testReleaseInstanceStepConcurrency() {
    // check unregistering step
    when(jobEvent1.toMaestroEvent(clusterName)).thenReturn(changeEvent1);
    when(jobEvent1.hasTerminal()).thenReturn(true);
    processor.process(() -> jobEvent1);
    verify(handler, times(1)).unregisterStep(jobEvent1.getCorrelationId(), jobEvent1.getStepUuid());

    // check instance cleanup
    when(changeEvent2.getNewStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    when(jobEvent2.getWorkflowId()).thenReturn(workflowId);
    when(jobEvent2.toMaestroEventStream(clusterName)).thenReturn(Stream.of(changeEvent2));
    doCallRealMethod().when(publisher).publishOrThrow(any());
    doCallRealMethod().when(publisher).publishOrThrow(any(), any());
    when(publisher.publish(any(), anyLong())).thenReturn(Optional.empty());
    processor.process(() -> jobEvent2);
    verify(handler, times(1)).cleanUp(changeEvent2.getCorrelationId());
    verify(handler, times(0)).removeInstance(any(), anyInt(), any());

    when(jobEvent2.toMaestroEventStream(clusterName)).thenReturn(Stream.of(changeEvent2));
    when(changeEvent2.getDepth()).thenReturn(2);
    processor.process(() -> jobEvent2);
    verify(handler, times(1)).cleanUp(changeEvent2.getCorrelationId());
    verify(handler, times(1))
        .removeInstance(
            changeEvent2.getCorrelationId(),
            changeEvent2.getDepth(),
            changeEvent2.getWorkflowUuid());
  }
}
