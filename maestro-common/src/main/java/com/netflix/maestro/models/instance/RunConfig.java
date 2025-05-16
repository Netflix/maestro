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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.definition.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.Data;

/**
 * Configuration setup for workflow definition or step definition during workflow start, workflow
 * restart, and step restart.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"runtime_tags", "correlation_id", "policy", "restart_config"},
    alphabetic = true)
@Data
public class RunConfig {
  @Valid private List<Tag> runtimeTags;
  private String correlationId;

  @NotNull private RunPolicy policy; // the policy used by the handler to restart this run

  // optional config used by the handler to restart this run
  @Nullable private RestartConfig restartConfig;

  /**
   * Optional step ids that it will begin with inclusively. It will often be derived from policy or
   * specified by callers for some policies. If not set (null), the run will start from the
   * beginning of the DAG. If set as empty, it will not run any steps. It is used internally.
   */
  @JsonIgnore @Nullable @Valid private List<String> startStepIds;

  /**
   * Optional step ids that it will end with inclusively. It will often be derived from policy or
   * specified by callers for some policies. If it is unset (null) or empty, the run will end at the
   * end of the DAG. It is used internally.
   */
  @JsonIgnore @Nullable @Valid private List<String> endStepIds;

  /**
   * Optional step ids that it will be skipped (marked as Noop). It will often be used to skip some
   * steps after users already manually run those. If it is unset (null) or empty, no specific step
   * will be marked as skipped. It is used internally.
   *
   * <p>skipStepIds is used to support skipping tasks.
   */
  @JsonIgnore @Nullable @Valid private List<String> skipStepIds;
}
