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
package com.netflix.maestro.models.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.timeline.TimelineEvent;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Activate/deactivate workflow action response. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "workflow_version_id", "timeline_event"},
    alphabetic = true)
@JsonDeserialize(builder = WorkflowActionResponse.WorkflowActionResponseBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class WorkflowActionResponse {
  private final String workflowId;
  private final long workflowVersionId;
  private final TimelineEvent timelineEvent;

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class WorkflowActionResponseBuilder {}

  /**
   * static method to create a {@link WorkflowActionResponse} from workflow id, status and timeline
   * and workflow_version_id = 0 {@com.netflix.maestro.models.Constants#INACTIVE_VERSION_ID} for
   * inactive version (used for constructing deactivate workflow response).
   */
  @JsonIgnore
  public static WorkflowActionResponse from(String workflowId, TimelineEvent timeline) {
    return WorkflowActionResponse.builder().workflowId(workflowId).timelineEvent(timeline).build();
  }

  /**
   * static method to create a {@link WorkflowActionResponse} from workflow id, version, status and
   * timeline.
   */
  @JsonIgnore
  public static WorkflowActionResponse from(
      String workflowId, long workflowVersionId, TimelineEvent timeline) {
    return WorkflowActionResponse.builder()
        .workflowId(workflowId)
        .workflowVersionId(workflowVersionId)
        .timelineEvent(timeline)
        .build();
  }
}
