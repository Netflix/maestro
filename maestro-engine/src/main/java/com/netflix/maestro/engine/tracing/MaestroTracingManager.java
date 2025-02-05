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

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import brave.sampler.SamplerFunction;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.instance.StepInstance;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages tracing operations inside maestro. Underlying uses zipkin's {@link Tracer} bean. More
 * details at <a href="https://github.com/openzipkin/brave">openzipkin</a>.
 */
@Slf4j
public abstract class MaestroTracingManager {

  private static final String INIT_SPAN_NAME = "maestro-step-attempt-init";
  private static final String RUN_SPAN_NAME = "maestro-step-attempt-run";

  private final Tracer tracer;
  private final SamplerFunction<Object> samplerFunctionAlways;
  private final Object samplerFunctionAlwaysArg;

  /** Constructor. */
  protected MaestroTracingManager(Tracer tracer) {
    this.tracer = tracer;
    this.samplerFunctionAlways = arg -> true;
    this.samplerFunctionAlwaysArg = ""; // sampler arg must not be null
  }

  /**
   * Tags the init span with the given {@link WorkflowSummary} and {@link StepRuntimeSummary}.
   *
   * @param initSpan init span
   * @param workflowSummary workflow summary
   * @param runtimeSummary runtime summary
   */
  public abstract void tagInitSpan(
      Span initSpan, WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary);

  /**
   * Initializes tracing and return a {@link MaestroTracingContext} representing the created trace.
   */
  public MaestroTracingContext initTracingContext(
      WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    try {
      final Span initSpan = tracer.nextSpan();
      try {
        initSpan.name(INIT_SPAN_NAME);
        tagInitSpan(initSpan, workflowSummary, runtimeSummary);
        initSpan.start();
      } finally {
        // always send the init Span asap with tags.
        initSpan.finish();
      }

      final TraceContext initContext = initSpan.context();
      LOG.trace(
          "Created initial span with traceID: {}, spanID: {}, maestro object {}, {}",
          initContext.traceIdString(),
          initContext.spanIdString(),
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity());

      // runSpan corresponds to the lifecycle of a step attempt.
      Span runSpan =
          tracer.nextSpanWithParent(samplerFunctionAlways, samplerFunctionAlwaysArg, initContext);
      runSpan.name(RUN_SPAN_NAME);
      TraceContext runContext = runSpan.context();
      LOG.trace(
          "Created run span with traceID: {}, spanID: {}, parentSpanID: {}",
          runContext.traceIdString(),
          runContext.spanIdString(),
          runContext.parentIdString());

      return MaestroTracingContext.fromTraceContext(runContext);
    } catch (Exception ex) {
      LOG.warn(
          "Exception caught when initializing trace for {}, {}",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          ex);
      return null;
    }
  }

  /** Annotates a {@link MaestroTracingContext}. */
  @VisibleForTesting
  void annotate(MaestroTracingContext tracingContext, String value) {
    try {
      if (tracingContext == null) {
        return;
      }
      Span span = getSpan(tracingContext);
      LOG.trace(
          "Annotating span traceID: {}, spanID: {} with value {}",
          span.context().traceIdString(),
          span.context().spanIdString(),
          value);
      span.annotate(value);
    } catch (Exception ex) {
      LOG.trace("Exception caught when annotating {} with {}", tracingContext, value, ex);
    }
  }

  private Span getSpan(MaestroTracingContext tracingContext) {
    TraceContext context = tracingContext.toTraceContext();
    return tracer.toSpan(context);
  }

  /**
   * Operates the tracing context based on step instance status. An exception can be included to
   * annotate the failure.
   */
  public void handleStepStatus(
      MaestroTracingContext tracingContext, StepInstance.Status status, Throwable throwable) {
    if (tracingContext == null || status == null) {
      return;
    }
    if (StepInstance.Status.CREATED.equals(status)) {
      start(tracingContext, status.name());
    } else if (status.isTerminal()) {
      finish(tracingContext, status.name(), throwable);
    } else {
      annotate(tracingContext, status.name());
    }
  }

  /** Operates the tracing context based on step instance status. */
  public void handleStepStatus(MaestroTracingContext tracingContext, StepInstance.Status status) {
    handleStepStatus(tracingContext, status, null);
  }

  /** Starts a {@link MaestroTracingContext}. */
  @VisibleForTesting
  void start(MaestroTracingContext tracingContext, String annotation) {
    try {
      if (tracingContext == null) {
        return;
      }
      Span span = getSpan(tracingContext);
      LOG.trace(
          "Starting span traceID: {}, spanID: {} with annotation {}",
          span.context().traceIdString(),
          span.context().spanIdString(),
          annotation);
      span.name(RUN_SPAN_NAME);
      span.annotate(annotation);
      span.start();
    } catch (Exception ex) {
      LOG.warn("Exception caught when starting {}", tracingContext, ex);
    }
  }

  /** Finishes a {@link MaestroTracingContext} with an exception. */
  @VisibleForTesting
  void finish(MaestroTracingContext tracingContext, String annotation, Throwable throwable) {
    try {
      if (tracingContext == null) {
        return;
      }
      Span span = getSpan(tracingContext);
      LOG.trace(
          "Finishing span traceID: {}, spanID: {} with annotation {}",
          span.context().traceIdString(),
          span.context().spanIdString(),
          annotation);
      span.annotate(annotation);
      if (throwable != null) {
        span.error(throwable);
      }
      span.finish();
    } catch (Exception ex) {
      LOG.warn("Exception caught when finishing {}", tracingContext, ex);
    }
  }
}
