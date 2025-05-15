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
package com.netflix.maestro.models.initiator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import lombok.Data;

/** Manual Initiator of a workflow instance. It is the default initiator type. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic = true)
@Data
public class ManualInitiator implements Initiator {
  private int depth;

  private User user; // if not provided, will be extracted from the cred.

  @Override
  public Type getType() {
    return Type.MANUAL;
  }

  @JsonIgnore
  @Override
  public TimelineEvent getTimelineEvent() {
    return TimelineLogEvent.info(
        "Manually run a new workflow instance by a user [%s]",
        user == null ? "unknown" : user.getName());
  }

  @Override
  public void setCaller(User caller) {
    this.user = caller;
  }
}
