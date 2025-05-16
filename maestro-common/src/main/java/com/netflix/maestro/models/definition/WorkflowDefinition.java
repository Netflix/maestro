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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.trigger.TriggerUuids;
import com.netflix.maestro.validations.WorkflowConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * This is the workflow definition data model pushed to and returned from users over API.
 *
 * <p>It contains all the data pushed by users. It also includes additional metadata info generated
 * by either DSL client or maestro service.
 *
 * <p>All data will be immutably stored as JSONB in maestro_workflow_version table.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "properties_snapshot",
      "metadata",
      "workflow",
      "is_active",
      "activate_time",
      "activated_by",
      "is_latest",
      "is_default",
      "internal_id",
      "enriched_extras",
      "modify_time"
    },
    alphabetic = true)
@Data
public class WorkflowDefinition {
  @Valid @NotNull private PropertiesSnapshot propertiesSnapshot;
  @Valid @NotNull private Metadata metadata;
  @Valid @WorkflowConstraint private Workflow workflow;

  private Boolean isActive = true; // active status of the associated workflow version
  private Long activateTime; // not null if activated, otherwise absent.
  private User activatedBy; // the caller info to activate the version if activated

  @NotNull private Boolean isLatest;
  // default version is either the active one or the latest one if no active
  @NotNull private Boolean isDefault;
  @NotNull private Long modifyTime; // the last modified timestamp in milliseconds
  @Valid private Long internalId; // internal unique sequence id

  // used internally and won't be returned by API
  @JsonIgnore @Valid private TriggerUuids triggerUuids;

  // extras added when enriched response
  @Nullable private WorkflowDefinitionExtras enrichedExtras;

  /** Gets current run strategy or default. */
  @JsonIgnore
  public RunStrategy getRunStrategyOrDefault() {
    return this.getPropertiesSnapshot().getRunStrategy() == null
        ? Defaults.DEFAULT_RUN_STRATEGY
        : this.getPropertiesSnapshot().getRunStrategy();
  }
}
