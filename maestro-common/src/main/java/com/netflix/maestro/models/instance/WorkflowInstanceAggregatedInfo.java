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
package com.netflix.maestro.models.instance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Data;

/**
 * Aggregated view stored by WorkflowInstance. It serves two purposes. First, when an instance run
 * starts/restarts, it keeps the aggregated info of all previous runs, i.e. the aggregated info from
 * run 1 to the previous run. It will be null if this is the first run. This aggregated info is
 * called baseline aggregated info and will be kept in DB in the workflow instance table. Second,
 * when any caller (users or other applications) asks for its aggregated info, baseline will then be
 * merged with the current run's real-time step status and workflow status to get the current actual
 * aggregated info. Note that only baseline is kept in DB and the real-time aggregated info will
 * always be computed at calling time. The real time workflowInstanceStatus is kept in workflow
 * instance status. The stored workflowInstanceStatus in DB is the aggregated status of previous run
 * and used for computing the current aggregated status. Maestro engine should not directly return
 * baseline workflowInstanceStatus to the callers. Instead, it should use workflow instance status
 * or run_status.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"workflow_instance_status", "step_aggregated_views"},
    alphabetic = true)
@Data
public class WorkflowInstanceAggregatedInfo {
  /** Default constructor. */
  public WorkflowInstanceAggregatedInfo() {
    stepAggregatedViews = new LinkedHashMap<>();
  }

  private Map<String, StepAggregatedView> stepAggregatedViews;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private WorkflowInstance.Status workflowInstanceStatus;
}
