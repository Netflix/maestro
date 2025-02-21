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
package com.netflix.maestro.timetrigger;

import java.util.concurrent.TimeUnit;

/** Constants for Maestro time trigger. */
public final class Constants {
  private Constants() {}

  /** Maestro workflow start time parameter name. */
  public static final String MAESTRO_RUN_PARAM_NAME = "RUN_TS";

  /** Message delay for immediate messages. */
  public static final int MESSAGE_DELAY_FIRST_EXECUTION = 10;

  /** Time trigger workflow version is always the active version. */
  public static final String TIME_TRIGGER_WORKFLOW_VERSION =
      com.netflix.maestro.models.Constants.WorkflowVersion.ACTIVE.name();

  /** Second to millisecond converter. */
  public static final long MILLISECONDS_CONVERT = TimeUnit.SECONDS.toMillis(1);
}
