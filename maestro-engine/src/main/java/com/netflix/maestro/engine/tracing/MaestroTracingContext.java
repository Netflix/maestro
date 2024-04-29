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

import brave.propagation.TraceContext;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.DefaultArtifact;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Class that holds tracing related information, allowing {@link MaestroTracingManager} to trace
 * step attempts.
 */
@JsonDeserialize(builder = MaestroTracingContext.MaestroTracingContextBuilder.class)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"trace_id_high", "trace_id_low", "span_id", "parent_span_id", "sampled"},
    alphabetic = true)
@Getter
@EqualsAndHashCode
@ToString
public final class MaestroTracingContext {

  private final long traceIdHigh;
  private final long traceIdLow;
  private final long spanId;
  private final long parentSpanId;
  private final boolean sampled;

  @Builder
  private MaestroTracingContext(
      long traceIdHigh, long traceIdLow, long spanId, long parentSpanId, boolean sampled) {
    this.traceIdHigh = traceIdHigh;
    this.traceIdLow = traceIdLow;
    this.spanId = spanId;
    this.parentSpanId = parentSpanId;
    this.sampled = sampled;
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  public static final class MaestroTracingContextBuilder {}

  /** Create a {@link MaestroTracingContext} from brave's {@link TraceContext}. */
  public static MaestroTracingContext fromTraceContext(TraceContext from) {
    MaestroTracingContextBuilder builder =
        MaestroTracingContext.builder().traceIdLow(from.traceId()).spanId(from.spanId());
    if (from.traceIdHigh() > 0) {
      builder.traceIdHigh(from.traceIdHigh());
    }
    if (from.parentId() != null) {
      builder.parentSpanId(from.parentId());
    }
    if (from.sampled() != null) {
      builder.sampled(from.sampled());
    }
    return builder.build();
  }

  /** Creates brave's {@link TraceContext} from the current {@link MaestroTracingContext} object. */
  public TraceContext toTraceContext() {
    TraceContext.Builder builder = TraceContext.newBuilder();
    builder.traceId(traceIdLow);
    if (traceIdHigh > 0) {
      builder.traceIdHigh(traceIdHigh);
    }
    if (parentSpanId > 0) {
      builder.parentId(parentSpanId);
    }
    builder.spanId(spanId);
    builder.sampled(sampled);
    return builder.build();
  }

  /** Create artifacts including an artifact to keep the tracing id. */
  public Map<String, Artifact> toTracingArtifacts() {
    String tracingId = toTraceContext().traceIdString();
    if (tracingId != null && !tracingId.isEmpty()) {
      Map<String, Artifact> artifactMap = new LinkedHashMap<>();
      artifactMap.put("tracing_context", DefaultArtifact.create("tracing_id", tracingId));
      return artifactMap;
    }
    return null;
  }
}
