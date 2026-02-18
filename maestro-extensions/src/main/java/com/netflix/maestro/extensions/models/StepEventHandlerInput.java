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
package com.netflix.maestro.extensions.models;

import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import com.netflix.maestro.models.instance.WorkflowInstance;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * A record to host all information of a {@link StepInstanceStatusChangeEvent} that can be used by
 * different event handlers independently.
 *
 * @param workflowInstance the workflow instance associated with step change event.
 * @param stepId id of the step.
 * @param stepAttemptId attempt id of the step.
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public record StepEventHandlerInput(
    @NotNull WorkflowInstance workflowInstance,
    @NotNull String stepId,
    @Min(1) long stepAttemptId) {}
