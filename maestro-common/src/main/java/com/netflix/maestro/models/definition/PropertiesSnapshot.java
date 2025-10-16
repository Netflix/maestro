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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.validations.MaestroIdConstraint;
import com.netflix.maestro.validations.TagListConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Workflow properties snapshot, which apply to all the versions of a workflow
 *
 * <p>If unset (null value), use the default (configurable) value.
 *
 * <p>Properties changes are kept separately and can evolve independently in Maestro.
 */
@JsonDeserialize(builder = PropertiesSnapshot.PropertiesSnapshotBuilder.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "create_time",
      "author",
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
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public final class PropertiesSnapshot {
  @MaestroIdConstraint private final String workflowId;
  @NotNull private final Long createTime; // this is served as the snapshot version
  @NotNull private final User author; // author info and it should be not null

  @NotNull @Valid private final User owner;
  @Valid private final AccessControl accessControl;
  @Valid private final RunStrategy runStrategy;
  private final Long stepConcurrency;
  @Valid private final Alerting alerting;
  private Boolean alertingDisabled;
  private Boolean signalTriggerDisabled;
  private Boolean timeTriggerDisabled;

  @Size(max = Constants.FIELD_SIZE_LIMIT)
  private String description;

  @Valid @TagListConstraint @Nullable private TagList tags;

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class PropertiesSnapshotBuilder {}

  /** static PropertiesSnapshot creator. */
  public static PropertiesSnapshot create(
      String workflowId, Long createTime, User author, Properties properties) {
    return PropertiesSnapshot.builder()
        .workflowId(workflowId)
        .createTime(createTime)
        .author(author)
        .owner(properties.getOwner())
        .accessControl(properties.getAccessControl())
        .runStrategy(properties.getRunStrategy())
        .stepConcurrency(properties.getStepConcurrency())
        .alerting(properties.getAlerting())
        .alertingDisabled(properties.getAlertingDisabled())
        .signalTriggerDisabled(properties.getSignalTriggerDisabled())
        .timeTriggerDisabled(properties.getTimeTriggerDisabled())
        .description(properties.getDescription())
        .tags(properties.getTags())
        .build();
  }

  /** extract properties info. */
  public Properties extractProperties() {
    Properties properties = new Properties();
    properties.setOwner(owner);
    properties.setAccessControl(accessControl);
    properties.setRunStrategy(runStrategy);
    properties.setStepConcurrency(stepConcurrency);
    properties.setAlerting(alerting);
    properties.setAlertingDisabled(alertingDisabled);
    properties.setSignalTriggerDisabled(signalTriggerDisabled);
    properties.setTimeTriggerDisabled(timeTriggerDisabled);
    properties.setDescription(description);
    properties.setTags(tags);
    return properties;
  }
}
