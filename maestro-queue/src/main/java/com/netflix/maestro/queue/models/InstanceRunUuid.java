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
package com.netflix.maestro.queue.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Wrapper class for workflow instance id and workflow instance run id and uuid. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonPropertyOrder(
    value = {"instance_id", "run_id", "uuid"},
    alphabetic = true)
@Getter
@ToString
@EqualsAndHashCode
public class InstanceRunUuid {
  private final long instanceId;
  private final long runId;
  private final String uuid;

  /** Constructor. */
  @JsonCreator
  public InstanceRunUuid(
      @JsonProperty("instance_id") long instanceId,
      @JsonProperty("run_id") long runId,
      @JsonProperty("uuid") String uuid) {
    this.instanceId = instanceId;
    this.runId = runId;
    this.uuid = uuid;
  }
}
