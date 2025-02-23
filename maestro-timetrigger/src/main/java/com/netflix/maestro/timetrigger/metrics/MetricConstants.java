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
package com.netflix.maestro.timetrigger.metrics;

/** Constants for time trigger metrics. */
public final class MetricConstants extends com.netflix.maestro.engine.metrics.MetricConstants {
  private MetricConstants() {}

  /** Create timetrigger subscription metrics key. */
  public static final String CREATE_SUBSCRIPTION_METRIC = "timetrigger.subscription.create";

  /** Launch workflow from timetrigger metrics key. */
  public static final String EXECUTION_TRIGGER_WORKFLOW_METRIC = "timetrigger.execution.trigger";

  /** Process timetrigger execution metrics key. */
  public static final String EXECUTION_PROCESS_METRIC = "timetrigger.execution.process";

  /** Process timetrigger execution trigger disabled metrics key. */
  public static final String EXECUTION_TRIGGER_DISABLED_METRIC =
      "timetrigger.execution.triggerdisabled";

  /** Timetrigger execution error metrics key. */
  public static final String EXECUTION_ERROR_METRIC = "timetrigger.execution.error";

  /** Terminate timetrigger subscription metrics key. */
  public static final String TERMINATE_SUBSCRIPTION_METRIC = "timetrigger.subscription.terminate";

  /** Tag value for not found error. */
  public static final String TAG_VALUE_NOT_FOUND = "notfound";

  /** Tag value for inactive workflow error. */
  public static final String TAG_VALUE_INACTIVE = "inactive";

  /** Tag value for unknown 404 error. */
  public static final String TAG_VALUE_UNKNOWN_404 = "notfoundunknown";

  /** Tag value for unknown 422 error. */
  public static final String TAG_VALUE_UNKNOWN_422 = "unprocessableunknown";

  /** Tag value for conflict error. */
  public static final String TAG_VALUE_CONFLICT = "conflict";

  /** Tag value for multiple executions triggers. */
  public static final String TAG_VALUE_BATCH = "batch";

  /** Tag value for single execution trigger. */
  public static final String TAG_VALUE_SINGLE = "single";
}
