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
package com.netflix.maestro.queue.processors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.queue.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.queue.jobevents.InstanceActionJobEvent;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.jobevents.NotificationJobEvent;
import com.netflix.maestro.queue.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.queue.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.TerminateThenRunJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowVersionUpdateJobEvent;
import java.util.EnumMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class MaestroJobEventDispatcherTest extends MaestroBaseTest {
  @Mock private MaestroEventProcessor<StartWorkflowJobEvent> startWorkflowJobEventProcessor;
  @Mock private MaestroEventProcessor<TerminateThenRunJobEvent> terminateThenRunJobEventProcessor;
  @Mock private MaestroEventProcessor<InstanceActionJobEvent> instanceActionJobEventProcessor;
  @Mock private MaestroEventProcessor<MaestroJobEvent> updateJobEventProcessor;
  @Mock private MaestroEventProcessor<NotificationJobEvent> notificationJobEventProcessor;
  @Mock private MaestroEventProcessor<DeleteWorkflowJobEvent> deleteWorkflowJobEventProcessor;

  private MaestroJobEventDispatcher jobEventDispatcher;

  @Before
  public void setUp() {
    EnumMap<MaestroJobEvent.Type, MaestroEventProcessor<? extends MaestroJobEvent>>
        eventProcessorMap = new EnumMap<>(MaestroJobEvent.Type.class);
    eventProcessorMap.put(MaestroJobEvent.Type.START_WORKFLOW, startWorkflowJobEventProcessor);
    eventProcessorMap.put(
        MaestroJobEvent.Type.TERMINATE_THEN_RUN, terminateThenRunJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.INSTANCE_ACTION, instanceActionJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.STEP_INSTANCE_UPDATE, updateJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.WORKFLOW_INSTANCE_UPDATE, updateJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.WORKFLOW_VERSION_UPDATE, updateJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.NOTIFICATION, notificationJobEventProcessor);
    eventProcessorMap.put(MaestroJobEvent.Type.DELETE_WORKFLOW, deleteWorkflowJobEventProcessor);
    jobEventDispatcher = new MaestroJobEventDispatcher(eventProcessorMap);
  }

  @Test
  public void testProcessJobEvent() {
    MaestroJobEvent event = Mockito.mock(StartWorkflowJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventDispatcher.processJobEvent(event);
    verify(startWorkflowJobEventProcessor, times(1)).process(any());
    event = Mockito.mock(TerminateThenRunJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventDispatcher.processJobEvent(event);
    verify(terminateThenRunJobEventProcessor, times(1)).process(any());
    event = Mockito.mock(InstanceActionJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventDispatcher.processJobEvent(event);
    verify(instanceActionJobEventProcessor, times(1)).process(any());
    event = Mockito.mock(NotificationJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventDispatcher.processJobEvent(event);
    verify(notificationJobEventProcessor, times(1)).process(any());
    event = Mockito.mock(DeleteWorkflowJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventDispatcher.processJobEvent(event);
    verify(deleteWorkflowJobEventProcessor, times(1)).process(any());

    event = Mockito.mock(StepInstanceUpdateJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventDispatcher.processJobEvent(event);
    event = Mockito.mock(WorkflowInstanceUpdateJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventDispatcher.processJobEvent(event);
    event = Mockito.mock(WorkflowVersionUpdateJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventDispatcher.processJobEvent(event);
    verify(updateJobEventProcessor, times(3)).process(any());
  }
}
