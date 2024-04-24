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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/** Workflow activation change event schema. It will be sent externally. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "author",
      "current_active_version_id",
      "previous_active_version_id",
      "cluster_name",
      "event_time",
      "sync_time",
      "send_time"
    },
    alphabetic = true)
@JsonDeserialize(
    builder = WorkflowActivationChangeEvent.WorkflowActivationChangeEventBuilderImpl.class)
@SuperBuilder(toBuilder = true)
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class WorkflowActivationChangeEvent extends WorkflowChangeEvent {
  private final long currentActiveVersionId; // current active workflow version id
  private final long previousActiveVersionId; // previous active workflow version id

  @Override
  public Type getType() {
    return Type.WORKFLOW_ACTIVATION_CHANGE_EVENT;
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  static final class WorkflowActivationChangeEventBuilderImpl
      extends WorkflowActivationChangeEventBuilder<
          WorkflowActivationChangeEvent, WorkflowActivationChangeEventBuilderImpl> {}
}
