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
package com.netflix.maestro.models.events;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.User;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/** Abstract workflow change event schema. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@SuperBuilder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
abstract class WorkflowChangeEvent implements MaestroEvent {
  @Valid @NotNull private final String workflowId;
  @NotNull private final String clusterName;

  private final User author; // the user who makes the call to change the workflow.
  @Valid @NotNull private final long eventTime; // the moment that the status change happened
  @Valid @NotNull private final long syncTime; // the moment that the event is synced internally
  @Valid @NotNull private final long sendTime; // the moment that the event is sent externally
}
