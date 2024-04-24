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
package com.netflix.maestro.models.trigger;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** Time Trigger Definition. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(name = "CRON", value = CronTimeTrigger.class),
  @JsonSubTypes.Type(name = "INTERVAL", value = IntervalTimeTrigger.class),
  @JsonSubTypes.Type(name = "PREDEFINED", value = PredefinedTimeTrigger.class)
})
public interface TimeTrigger {
  /** Constant for millis to second translation. */
  long MS_IN_SECONDS = 1000L;

  /** Get TimeTrigger type info. */
  TimeTrigger.Type getType();

  /** Get Timezone for trigger. */
  String getTimezone();

  /** Supported trigger types. */
  enum Type {
    /** Cron time trigger. */
    CRON,
    /** Interval based time trigger. */
    INTERVAL,
    /** Predefined time trigger. */
    PREDEFINED;
  }
}
