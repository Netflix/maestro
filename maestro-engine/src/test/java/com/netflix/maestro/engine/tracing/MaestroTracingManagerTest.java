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
package com.netflix.maestro.engine.tracing;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.instance.StepInstance;
import org.junit.Before;
import org.junit.Test;

public class MaestroTracingManagerTest {

  private Tracer mockTracer;
  private Span mockSpan;
  private MaestroTracingContext defaultContext;

  private static class TestTracingManager extends MaestroTracingManager {
    public TestTracingManager(Tracer tracer) {
      super(tracer);
    }

    @Override
    public void tagInitSpan(
        Span initSpan, WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
      initSpan.tag("id", workflowSummary.getWorkflowId());
      initSpan.tag("id", workflowSummary.getWorkflowId());
      initSpan.tag("id", workflowSummary.getWorkflowId());
      initSpan.tag("id", workflowSummary.getWorkflowId());
      initSpan.tag("id", workflowSummary.getWorkflowId());
      initSpan.tag("id", workflowSummary.getWorkflowId());
      initSpan.tag("id", workflowSummary.getWorkflowId());
      initSpan.tag("id", workflowSummary.getWorkflowId());
    }
  }

  @Before
  public void setup() {
    mockTracer = mock(Tracer.class);
    mockSpan = mock(Span.class);
    defaultContext =
        MaestroTracingContext.builder()
            .traceIdHigh(100L)
            .traceIdLow(10L)
            .spanId(30L)
            .parentSpanId(200L)
            .build();
    when(mockTracer.toSpan(defaultContext.toTraceContext())).thenReturn(mockSpan);
    when(mockSpan.context()).thenReturn(defaultContext.toTraceContext());
  }

  @Test
  public void testInitTracingContext() {
    Span initSpan = mock(Span.class);
    TraceContext initContext = TraceContext.newBuilder().traceId(49L).spanId(50L).build();
    when(initSpan.context()).thenReturn(initContext);

    when(mockTracer.nextSpan()).thenReturn(initSpan);
    MaestroTracingManager tm = new TestTracingManager(mockTracer);

    Span runSpan = mock(Span.class);
    TraceContext runContext =
        TraceContext.newBuilder()
            .traceId(1L)
            .traceIdHigh(2L)
            .spanId(3L)
            .parentId(4L)
            .sampled(null)
            .build();
    when(runSpan.context()).thenReturn(runContext);
    when(mockTracer.nextSpanWithParent(any(), any(), any())).thenReturn(runSpan);

    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId("wf");
    workflowSummary.setWorkflowInstanceId(123L);
    workflowSummary.setWorkflowRunId(1L);
    workflowSummary.setWorkflowUuid("wf-uuid");

    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId("step")
            .stepInstanceId(456L)
            .stepAttemptId(3L)
            .stepInstanceUuid("step-uuid")
            .build();

    MaestroTracingContext context = tm.initTracingContext(workflowSummary, runtimeSummary);
    verify(initSpan, times(1)).name(any());
    verify(initSpan, atLeast(8)).tag(any(), any());
    verify(initSpan, times(1)).start();
    verify(initSpan, times(1)).finish();

    verify(runSpan, times(1)).name(any());
    assertEquals(context.toTraceContext(), runContext);
  }

  @Test
  public void testAnnotate() {
    MaestroTracingManager tm = new TestTracingManager(mockTracer);
    String value = "blah";
    tm.annotate(defaultContext, value);
    verify(mockSpan, times(1)).annotate(value);
  }

  @Test
  public void testStartSpan() {
    TestTracingManager tm = new TestTracingManager(mockTracer);
    String value = "blah";
    tm.start(defaultContext, value);
    verify(mockSpan, times(1)).annotate(value);
    verify(mockSpan, times(1)).start();
  }

  @Test
  public void testFinishSpan() {
    MaestroTracingManager tm = new TestTracingManager(mockTracer);
    String value = "blah";
    tm.finish(defaultContext, value, null);
    verify(mockSpan, times(1)).annotate(value);
    verify(mockSpan, times(1)).finish();
  }

  @Test
  public void testHandleStepStatus() {
    MaestroTracingManager tm = new TestTracingManager(mockTracer);
    int finishCount = 0;
    for (StepInstance.Status s : StepInstance.Status.values()) {
      System.out.println("Testing " + s.name());
      tm.handleStepStatus(defaultContext, s);
      verify(mockSpan, times(1)).annotate(s.name());

      if (s.isTerminal()) {
        verify(mockSpan, times(++finishCount)).finish();
      }
      if (s.equals(StepInstance.Status.CREATED)) {
        verify(mockSpan, times(1)).start();
      }
    }
  }
}
