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
package com.netflix.maestro.engine.listeners;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import com.netflix.maestro.engine.jobevents.RunWorkflowInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.StepInstanceWakeUpEvent;
import com.netflix.maestro.engine.jobevents.TerminateInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.TerminateThenRunInstanceJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.engine.processors.DeleteWorkflowJobProcessor;
import com.netflix.maestro.engine.processors.PublishJobEventProcessor;
import com.netflix.maestro.engine.processors.RunWorkflowInstancesJobProcessor;
import com.netflix.maestro.engine.processors.StartWorkflowJobProcessor;
import com.netflix.maestro.engine.processors.StepInstanceWakeUpEventProcessor;
import com.netflix.maestro.engine.processors.TerminateInstancesJobProcessor;
import com.netflix.maestro.engine.processors.TerminateThenRunInstanceJobProcessor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class InMemoryJobEventListenerTest extends MaestroEngineBaseTest {

  @Mock private DeleteWorkflowJobProcessor deleteWorkflowJobProcessor;
  @Mock private RunWorkflowInstancesJobProcessor runWorkflowInstancesJobProcessor;
  @Mock private StartWorkflowJobProcessor startWorkflowJobProcessor;
  @Mock private TerminateInstancesJobProcessor terminateInstancesJobProcessor;
  @Mock private TerminateThenRunInstanceJobProcessor terminateThenRunInstanceJobProcessor;
  @Mock private StepInstanceWakeUpEventProcessor stepInstanceWakeUpEventProcessor;
  @Mock private PublishJobEventProcessor publishJobEventProcessor;
  @Mock private LinkedBlockingQueue<MaestroJobEvent> queue;
  @Mock private ExecutorService executorService;
  private InMemoryJobEventListener jobEventListener;

  @Before
  public void setUp() throws Exception {
    jobEventListener =
        new InMemoryJobEventListener(
            deleteWorkflowJobProcessor,
            runWorkflowInstancesJobProcessor,
            startWorkflowJobProcessor,
            terminateInstancesJobProcessor,
            terminateThenRunInstanceJobProcessor,
            stepInstanceWakeUpEventProcessor,
            publishJobEventProcessor,
            queue,
            executorService);
  }

  @Test
  public void testProcess() {
    MaestroJobEvent event = Mockito.mock(DeleteWorkflowJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventListener.process(event);
    verify(deleteWorkflowJobProcessor, times(1)).process(any());
    event = Mockito.mock(RunWorkflowInstancesJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventListener.process(event);
    verify(runWorkflowInstancesJobProcessor, times(1)).process(any());
    event = Mockito.mock(StartWorkflowJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventListener.process(event);
    verify(startWorkflowJobProcessor, times(1)).process(any());
    event = Mockito.mock(TerminateInstancesJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventListener.process(event);
    verify(terminateInstancesJobProcessor, times(1)).process(any());
    event = Mockito.mock(TerminateThenRunInstanceJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventListener.process(event);
    verify(terminateThenRunInstanceJobProcessor, times(1)).process(any());
    event = Mockito.mock(StepInstanceWakeUpEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventListener.process(event);
    verify(stepInstanceWakeUpEventProcessor, times(1)).process(any());

    event = Mockito.mock(StepInstanceUpdateJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventListener.process(event);
    event = Mockito.mock(WorkflowInstanceUpdateJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventListener.process(event);
    event = Mockito.mock(WorkflowVersionUpdateJobEvent.class);
    when(event.getType()).thenCallRealMethod();
    jobEventListener.process(event);
    verify(publishJobEventProcessor, times(3)).process(any());
  }
}
