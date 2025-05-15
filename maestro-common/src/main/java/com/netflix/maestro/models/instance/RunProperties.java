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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.Alerting;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.definition.User;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/** Properties snapshot when the workflow instance starts. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"create_time", "owner", "alerting", "step_concurrency", "tags"},
    alphabetic = true)
@Data
public class RunProperties {
  @NotNull private Long createTime; // this is served as the snapshot version

  @NotNull @Valid private User owner; // only need owner name and trim extra info
  private Alerting alerting;
  private Long stepConcurrency;

  @Valid private TagList tags;

  /** static creator to extract un-evaluated properties from snapshot. */
  public static RunProperties from(PropertiesSnapshot snapshot) {
    RunProperties runProperties = new RunProperties();
    runProperties.setCreateTime(snapshot.getCreateTime());
    runProperties.setOwner(User.builder().name(snapshot.getOwner().getName()).build());
    runProperties.setAlerting(snapshot.getAlerting());
    runProperties.setStepConcurrency(snapshot.getStepConcurrency());
    runProperties.setTags(snapshot.getTags());
    return runProperties;
  }
}
