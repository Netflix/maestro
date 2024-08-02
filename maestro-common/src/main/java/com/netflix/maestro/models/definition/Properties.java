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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.validations.TagListConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

/**
 * Workflow properties changes, which is submitted by users to generate a new workflow properties
 * snapshot.
 *
 * <p>If unset (null value), means there is no change for this field.
 *
 * <p>Properties changes are kept separately and can evolve independently from the workflow version
 * changes.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "owner",
      "access_control",
      "run_strategy",
      "step_concurrency",
      "alerting",
      "alerting_disabled",
      "signal_trigger_disabled",
      "time_trigger_disabled",
      "description",
      "tags"
    },
    alphabetic = true)
@Data
public class Properties {
  @Valid private User owner;
  @Valid private AccessControl accessControl;
  @Valid private RunStrategy runStrategy;
  private Long stepConcurrency;
  @Valid private Alerting alerting;
  private Boolean alertingDisabled;
  private Boolean signalTriggerDisabled;
  private Boolean timeTriggerDisabled;

  @Size(max = Constants.FIELD_SIZE_LIMIT)
  private String description;

  @Valid @TagListConstraint @Nullable private TagList tags;

  /** merge the properties changes into current properties-snapshot. */
  public static Properties merge(@NotNull Properties change, Properties current) {
    Checks.notNull(change, "cannot merge a null change with the current properties");
    if (current == null) {
      return change;
    }

    Properties newSnapshot = new Properties();
    newSnapshot.setOwner(change.owner != null ? change.owner : current.owner);
    newSnapshot.setAccessControl(
        change.accessControl != null ? change.accessControl : current.accessControl);
    newSnapshot.setRunStrategy(
        change.runStrategy != null ? change.runStrategy : current.runStrategy);
    newSnapshot.setStepConcurrency(
        change.stepConcurrency != null ? change.stepConcurrency : current.stepConcurrency);
    newSnapshot.setAlerting(change.alerting != null ? change.alerting : current.alerting);
    newSnapshot.setAlertingDisabled(
        change.alertingDisabled != null ? change.alertingDisabled : current.alertingDisabled);
    newSnapshot.setSignalTriggerDisabled(
        change.signalTriggerDisabled != null
            ? change.signalTriggerDisabled
            : current.signalTriggerDisabled);
    newSnapshot.setTimeTriggerDisabled(
        change.timeTriggerDisabled != null
            ? change.timeTriggerDisabled
            : current.timeTriggerDisabled);
    newSnapshot.setDescription(
        change.description != null ? change.description : current.description);
    newSnapshot.setTags(change.tags != null ? change.tags : current.tags);
    return newSnapshot;
  }
}
