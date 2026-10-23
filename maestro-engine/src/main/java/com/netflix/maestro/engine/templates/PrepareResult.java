/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.engine.templates;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.models.stepruntime.TemplateExpansion;
import com.netflix.maestro.models.timeline.TimelineEvent;

/**
 * Outcome of one {@link TemplatePreparer#prepare} call.
 *
 * <p>A ready result carries the expansion. A pending result carries {@link
 * StepRuntime.State#CONTINUE} and says whether the artifact changed and must be persisted. A failed
 * result carries the terminal state the template step ends in.
 *
 * @param expansion the prepared expansion, null unless ready
 * @param state CONTINUE while pending, DONE when ready, otherwise the failure state
 * @param artifactChanged true when the context's artifact must be persisted with this result
 * @param event an optional timeline event for the template step
 */
public record PrepareResult(
    @Nullable TemplateExpansion expansion,
    StepRuntime.State state,
    boolean artifactChanged,
    @Nullable TimelineEvent event) {

  /** The expansion is ready, with an optional timeline event describing how it was produced. */
  public static PrepareResult ready(TemplateExpansion expansion, @Nullable TimelineEvent event) {
    return new PrepareResult(expansion, StepRuntime.State.DONE, false, event);
  }

  /** The expansion is not ready yet; the runtime will call prepare again later. */
  public static PrepareResult pending(TimelineEvent event, boolean artifactChanged) {
    return new PrepareResult(null, StepRuntime.State.CONTINUE, artifactChanged, event);
  }

  /** Preparation failed and the template step ends in the given terminal state. */
  public static PrepareResult failed(StepRuntime.State state, TimelineEvent event) {
    return new PrepareResult(null, state, false, event);
  }

  public boolean isReady() {
    return expansion != null;
  }
}
