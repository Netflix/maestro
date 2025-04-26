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
package com.netflix.maestro.queue.properties;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;

/** Maestro internal queue properties. */
@Getter
@Setter
public class QueueProperties {
  @Getter
  @Setter
  public static class QueueWorkerProperties {
    private static final int DEFAULT_WORKER_PER_TYPE = 3;
    private static final int DEFAULT_BATCH_QUERY_LIMIT = 100;
    private static final int DEFAULT_MESSAGE_LIMIT = 300;
    private static final long DEFAULT_OWNERSHIP_TIMEOUT_IN_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final long DEFAULT_EXECUTION_BUDGET_IN_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final long DEFAULT_SCAN_INTERVAL_IN_MILLIS = TimeUnit.SECONDS.toMillis(1);
    private static final long DEFAULT_RETRY_INTERVAL_IN_MILLIS = TimeUnit.SECONDS.toMillis(2);

    private int workerNum = DEFAULT_WORKER_PER_TYPE;
    private int batchLimit = DEFAULT_BATCH_QUERY_LIMIT;
    private int messageLimit = DEFAULT_MESSAGE_LIMIT;
    private long ownershipTimeout = DEFAULT_OWNERSHIP_TIMEOUT_IN_MILLIS;
    private long executionBudget = DEFAULT_EXECUTION_BUDGET_IN_MILLIS;
    private long scanInterval = DEFAULT_SCAN_INTERVAL_IN_MILLIS;
    private long retryInterval = DEFAULT_RETRY_INTERVAL_IN_MILLIS;
  }

  private static final QueueWorkerProperties DEFAULT_QUEUE_WORKER_PROPERTIES =
      new QueueWorkerProperties();

  private Map<Integer, QueueWorkerProperties> properties = new HashMap<>();

  public QueueWorkerProperties getQueueWorkerProperties(Integer queueId) {
    return properties.getOrDefault(queueId, DEFAULT_QUEUE_WORKER_PROPERTIES);
  }
}
