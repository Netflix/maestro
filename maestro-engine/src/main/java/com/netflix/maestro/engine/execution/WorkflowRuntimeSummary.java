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
package com.netflix.maestro.engine.execution;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineEvent;
import jakarta.validation.Valid;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Data;

/**
 * Workflow instance runtime summary, which carries runtime states persisted in the special
 * DEFAULT_END_STEP_NAME task output.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "instance_status",
      "start_time",
      "end_time",
      "modify_time",
      "runtime_overview",
      "rollup_base",
      "artifacts",
      "timeline"
    },
    alphabetic = true)
@Data
public class WorkflowRuntimeSummary {
  private WorkflowInstance.Status instanceStatus = WorkflowInstance.Status.CREATED; // initial state

  private Long startTime;
  private Long endTime;
  private Long modifyTime;

  @Valid private WorkflowRuntimeOverview runtimeOverview;

  /**
   * Contains subworkflow and foreach steps missing from the current run and regular steps missing
   * from the current run in one aggregated rollup base.
   */
  private WorkflowRollupOverview rollupBase;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Valid
  private Map<String, Artifact> artifacts = new LinkedHashMap<>();

  private Timeline timeline = new Timeline(null);

  /** Update runtime state within runtime summary. */
  public void updateRuntimeState(
      WorkflowInstance.Status status, WorkflowRuntimeOverview overview, Long curTime) {
    if (WorkflowInstance.Status.IN_PROGRESS.equals(status)) {
      this.startTime = curTime;
    } else {
      this.endTime = curTime;
    }
    this.instanceStatus = status;
    this.modifyTime = System.currentTimeMillis();
    this.runtimeOverview = overview;
  }

  /** Add an event to the timeline if not identical to the latest event. */
  public void addTimeline(TimelineEvent event) {
    timeline.add(event);
  }
}
