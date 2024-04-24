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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.time.Instant;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Wrapper for the signalInstanceID and signalInstance created timestamp. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonPropertyOrder(alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Getter
@Builder
@ToString
@EqualsAndHashCode
public class SignalReference {
  private final String signalInstanceId;
  private final Instant timestamp;

  public long getTimestamp() {
    return timestamp.toEpochMilli();
  }

  /**
   * Constructor with long timestamp.
   *
   * @param signalInstanceId signal instance id
   * @param timestamp timestamp
   */
  @JsonCreator
  public SignalReference(
      @JsonProperty("signal_instance_id") String signalInstanceId,
      @JsonProperty("timestamp") long timestamp) {
    this.signalInstanceId = signalInstanceId;
    this.timestamp = Instant.ofEpochMilli(timestamp);
  }

  /**
   * Constructor.
   *
   * @param signalInstanceId signal instance id
   * @param timestamp timestamp
   */
  public SignalReference(String signalInstanceId, Instant timestamp) {
    this.signalInstanceId = signalInstanceId;
    this.timestamp = timestamp;
  }
}
