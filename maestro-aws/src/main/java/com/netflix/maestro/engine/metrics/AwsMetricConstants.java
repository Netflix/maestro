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
package com.netflix.maestro.engine.metrics;

import com.netflix.maestro.metrics.MetricConstants;

/** Class for SQS related Metric constants. */
public class AwsMetricConstants extends MetricConstants {
  protected AwsMetricConstants() {}

  /** SQS publish success metric for time trigger. */
  public static final String SQS_TIME_TRIGGER_PUBLISH_SUCCESS_METRIC =
      "sqs.timetrigger.publish.success";

  /** SQS publish failures metric for time trigger. */
  public static final String SQS_TIME_TRIGGER_PUBLISH_FAILURE_METRIC =
      "sqs.timetrigger.publish.failure";

  /** SQS publish success metric for time trigger. */
  public static final String SQS_SIGNAL_PUBLISH_SUCCESS_METRIC = "sqs.signal.publish.success";

  /** SQS publish failures metric for time trigger. */
  public static final String SQS_SIGNAL_PUBLISH_FAILURE_METRIC = "sqs.signal.publish.failure";

  /** Metrics for instance_step_concurrency error count. */
  public static final String INSTANCE_STEP_CONCURRENCY_ERROR_METRIC =
      "instance.step.concurrency.error";
}
