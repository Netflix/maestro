/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.queue.jobevents;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

/**
 * Maestro internal job event to inform run strategy to try to start workflow instances for the
 * given workflow id. It is sent after creating queued workflow instances. It can be used to start a
 * single workflow instance as well.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"event"},
    alphabetic = true)
@Data
public class NotificationJobEvent implements MaestroJobEvent {
  private MaestroJobEvent event;

  /** static creator. */
  public static NotificationJobEvent create(MaestroJobEvent event) {
    NotificationJobEvent jobEvent = new NotificationJobEvent();
    jobEvent.setEvent(event);
    return jobEvent;
  }

  @Override
  public Type getType() {
    return Type.NOTIFICATION;
  }
}
