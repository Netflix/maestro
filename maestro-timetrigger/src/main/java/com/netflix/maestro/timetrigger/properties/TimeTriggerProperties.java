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
package com.netflix.maestro.timetrigger.properties;

import lombok.Getter;
import lombok.Setter;

/** Time trigger properties. */
@Getter
@Setter
public class TimeTriggerProperties {
  private static final int DEFAULT_TRIGGER_BATCH_SIZE = 10;
  private static final int DEFAULT_MAX_TRIGGERS_PER_MESSAGE = 100;
  private static final int DEFAULT_MAX_DELAY = 60 * 15;
  private static final int DEFAULT_MAX_JITTER = 240;

  private int triggerBatchSize = DEFAULT_TRIGGER_BATCH_SIZE;
  private int maxTriggersPerMessage = DEFAULT_MAX_TRIGGERS_PER_MESSAGE;
  private int maxDelay = DEFAULT_MAX_DELAY;
  private int maxJitter = DEFAULT_MAX_JITTER;
}
