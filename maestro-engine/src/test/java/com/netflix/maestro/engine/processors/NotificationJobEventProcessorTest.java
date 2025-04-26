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
package com.netflix.maestro.engine.processors;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import com.netflix.maestro.models.events.WorkflowInstanceStatusChangeEvent;
import com.netflix.maestro.queue.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.queue.jobevents.NotificationJobEvent;
import com.netflix.maestro.queue.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowVersionUpdateJobEvent;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class NotificationJobEventProcessorTest extends MaestroEngineBaseTest {

  @Mock private MaestroNotificationPublisher eventClient;

  @Mock private StepInstanceUpdateJobEvent jobEvent1;
  @Mock private StepInstanceStatusChangeEvent changeEvent1;
  @Mock private WorkflowInstanceUpdateJobEvent jobEvent2;
  @Mock private WorkflowInstanceStatusChangeEvent changeEvent2;
  @Mock private WorkflowVersionUpdateJobEvent jobEvent3;
  @Mock private DeleteWorkflowJobEvent jobEvent4;

  private final String clusterName = "test-cluster";
  private NotificationJobEventProcessor processor;

  @Before
  public void before() throws Exception {
    processor = new NotificationJobEventProcessor(eventClient, clusterName);
    when(jobEvent1.getType()).thenCallRealMethod();
    when(jobEvent2.getType()).thenCallRealMethod();
    when(jobEvent3.getType()).thenCallRealMethod();
    when(jobEvent3.getWorkflowId()).thenReturn("foo");
    when(jobEvent4.getType()).thenCallRealMethod();
  }

  @Test
  public void testConsumeStepInstanceUpdateJobEvent() {
    when(jobEvent1.toMaestroEvent(clusterName)).thenReturn(changeEvent1);
    Assert.assertTrue(processor.process(NotificationJobEvent.create(jobEvent1)).isEmpty());
    verify(jobEvent1, times(1)).getType();
    verify(eventClient, times(1)).send(changeEvent1);
  }

  @Test
  public void testConsumeWorkflowInstanceUpdateJobEvent() {
    when(jobEvent2.toMaestroEventStream(clusterName)).thenReturn(Stream.of(changeEvent2));
    Assert.assertTrue(processor.process(NotificationJobEvent.create(jobEvent2)).isEmpty());
    verify(jobEvent2, times(1)).getType();
    verify(eventClient, times(1)).send(changeEvent2);
  }

  @Test
  public void testConsumeWorkflowVersionJobEvent() {
    when(jobEvent3.toMaestroEvent(clusterName)).thenReturn(null);
    Assert.assertTrue(processor.process(NotificationJobEvent.create(jobEvent3)).isEmpty());
    verify(jobEvent3, times(1)).getType();
    verify(eventClient, times(1)).send(null);
  }

  @Test
  public void testConsumeDeleteWorkflowJobEvent() {
    when(jobEvent4.toMaestroEvent(clusterName)).thenReturn(null);
    Assert.assertTrue(processor.process(NotificationJobEvent.create(jobEvent4)).isEmpty());
    verify(jobEvent4, times(1)).getType();
    verify(eventClient, times(1)).send(null);
  }
}
