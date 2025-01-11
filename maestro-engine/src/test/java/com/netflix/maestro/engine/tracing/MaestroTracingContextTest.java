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
import static org.junit.Assert.assertTrue;

import brave.propagation.TraceContext;
import com.netflix.maestro.models.artifact.DefaultArtifact;
import java.util.Collections;
import org.junit.Test;

public class MaestroTracingContextTest {

  @Test
  public void testTraceContextTransform() {
    long traceIdHigh = 123L;
    long traceIdLow = 456L;
    long spanId = 789L;
    long parentId = 999L;
    TraceContext from =
        TraceContext.newBuilder()
            .traceId(traceIdLow)
            .traceIdHigh(traceIdHigh)
            .spanId(spanId)
            .parentId(parentId)
            .sampled(true)
            .build();
    MaestroTracingContext actual = MaestroTracingContext.fromTraceContext(from);
    assertEquals(traceIdHigh, actual.getTraceIdHigh());
    assertEquals(traceIdLow, actual.getTraceIdLow());
    assertEquals(spanId, actual.getSpanId());
    assertEquals(parentId, actual.getParentSpanId());
    assertTrue(actual.isSampled());

    TraceContext to = actual.toTraceContext();
    assertEquals(from, to);
  }

  @Test
  public void testGenerateTracingArtifact() {
    MaestroTracingContext traceContext =
        MaestroTracingContext.fromTraceContext(
            TraceContext.newBuilder()
                .traceId(456L)
                .traceIdHigh(123L)
                .spanId(789L)
                .parentId(999L)
                .sampled(true)
                .build());
    DefaultArtifact expected =
        DefaultArtifact.create("tracing_id", "000000000000007b00000000000001c8");
    expected.add("context", traceContext);
    assertEquals(
        Collections.singletonMap("tracing_context", expected), traceContext.toTracingArtifacts());
  }
}
