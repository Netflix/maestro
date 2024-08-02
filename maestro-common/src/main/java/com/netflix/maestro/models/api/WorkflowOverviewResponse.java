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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.instance.WorkflowInstance;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.EnumMap;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Workflow overview, including run strategy, current non-terminal running instance stats (last run
 * is in non-terminal state), failed instance number (last run is in failed state), and the
 * latest_instance_id (indicating the total number of created instances for this workflow id).
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "active_version_id",
      "latest_version_id",
      "default_version_id",
      "properties_snapshot",
      "step_concurrency",
      "nonterminal_instances",
      "failed_instances",
      "latest_instance_id"
    },
    alphabetic = true)
@JsonDeserialize(builder = WorkflowOverviewResponse.WorkflowOverviewResponseBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class WorkflowOverviewResponse {
  @Valid @NotNull private final String workflowId;
  private final Long activeVersionId;
  private final Long latestVersionId;
  private final Long defaultVersionId;
  private final PropertiesSnapshot propertiesSnapshot;
  private final Long stepConcurrency;
  private final EnumMap<WorkflowInstance.Status, Long> nonterminalInstances;
  private final Long failedInstances;
  private final Long latestInstanceId;

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class WorkflowOverviewResponseBuilder {}
}
