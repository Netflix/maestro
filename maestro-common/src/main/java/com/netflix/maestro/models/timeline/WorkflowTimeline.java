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
package com.netflix.maestro.models.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.User;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Workflow timeline to record workflow level change history. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "timeline_events"},
    alphabetic = true)
@Getter
@EqualsAndHashCode
public class WorkflowTimeline {
  private final String workflowId;
  private final List<WorkflowTimelineEvent> timelineEvents;

  /** constructor and JSON serde. */
  @JsonCreator
  public WorkflowTimeline(
      @JsonProperty("workflow_id") String workflowId,
      @JsonProperty("timeline_events") List<WorkflowTimelineEvent> timelineEvents) {
    this.workflowId = workflowId;
    this.timelineEvents = timelineEvents;
  }

  /** Workflow timeline event to record a workflow change event. */
  @Getter
  @EqualsAndHashCode
  public static class WorkflowTimelineEvent {
    private final User author;
    private final String log;
    private final long timestamp;

    /** constructor and JSON serde. */
    @JsonCreator
    public WorkflowTimelineEvent(
        @JsonProperty("author") User author,
        @JsonProperty("log") String log,
        @JsonProperty("timestamp") long timestamp) {
      this.author = author;
      this.log = log;
      this.timestamp = timestamp;
    }
  }
}
