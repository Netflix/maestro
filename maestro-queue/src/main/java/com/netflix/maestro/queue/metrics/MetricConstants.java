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
package com.netflix.maestro.queue.metrics;

/** Class for Metric constants such as keys / tags for queue package. */
public final class MetricConstants extends com.netflix.maestro.metrics.MetricConstants {

  /** Constructor for MetricConstants. */
  private MetricConstants() {}

  /** tag to indicate if the error is retryable. */
  public static final String RETRYABLE_TAG = "retryable";

  /** Metric to count for the worker queue size. */
  public static final String WORKER_QUEUE_SIZE = "maestro.worker.queue.size";

  /** Metric to record the queueing delay time for a worker queue. */
  public static final String WORKER_QUEUE_QUEUEING_DELAY = "maestro.worker.queue.queueing.delay";

  /** Metric to count for the queue worker internal error. */
  public static final String QUEUE_WORKER_INTERNAL_ERROR = "maestro.queue.worker.internal.error";

  /** Metric to count for the queue system enqueue transaction. */
  public static final String QUEUE_SYSTEM_ENQUEUE_TRANSACTION =
      "maestro.queue.system.enqueue.transaction";

  /** Metric to count for the queue system enqueue error. */
  public static final String QUEUE_SYSTEM_ENQUEUE_ERROR = "maestro.queue.system.enqueue.error";

  /** Metric to count for the queue system enqueue. */
  public static final String QUEUE_SYSTEM_ENQUEUE = "maestro.queue.system.enqueue";

  /** Metric to count for the queue system notify. */
  public static final String QUEUE_SYSTEM_NOTIFY = "maestro.queue.system.notify";

  /** Metric to count for the queue system notify error. */
  public static final String QUEUE_SYSTEM_NOTIFY_ERROR = "maestro.queue.system.notify.error";
}
