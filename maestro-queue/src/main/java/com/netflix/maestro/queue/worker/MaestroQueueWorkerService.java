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
package com.netflix.maestro.queue.worker;

import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.queue.dao.MaestroQueueDao;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.queue.processors.MaestroJobEventDispatcher;
import com.netflix.maestro.queue.properties.QueueProperties;
import com.netflix.maestro.utils.Checks;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MaestroQueueWorkerService {
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final ExecutorService workers; // worker pools for each job type

  @SuppressWarnings({"PMD.LooseCoupling", "PMD.AvoidInstantiatingObjectsInLoops"})
  public MaestroQueueWorkerService(
      EnumMap<MaestroJobEvent.Type, BlockingQueue<MessageDto>> eventQueues,
      MaestroJobEventDispatcher dispatcher,
      MaestroQueueDao queueDao,
      QueueProperties properties,
      MaestroMetrics metrics) {
    Checks.checkTrue(
        Objects.equals(eventQueues.keySet(), dispatcher.typesSupported()),
        "Event types for event queues [%s] and processors [%s] must match",
        eventQueues.keySet(),
        dispatcher.typesSupported());
    int totalWorkerNum =
        eventQueues.keySet().stream()
            .map(MaestroJobEvent.Type::getQueueId)
            .distinct()
            .mapToInt(queueId -> properties.getQueueWorkerProperties(queueId).getWorkerNum())
            .sum();
    this.workers = Executors.newFixedThreadPool(totalWorkerNum);

    // create all the queue workers based on configs and share the same queue for the same queue id
    Set<Integer> visited = new HashSet<>();
    for (var entry : eventQueues.keySet()) {
      var messageQueue =
          Checks.notNull(eventQueues.get(entry), "cannot find queue for event type [%s]", entry);
      Integer queueId = entry.getQueueId();
      if (!visited.contains(queueId)) {
        visited.add(queueId);
        // boostrap the queue with a message to trigger the worker
        messageQueue.offer(MessageDto.SCAN_CMD_MSG);
        queueDao.addLockIfAbsent(queueId);
        var workerProperties = properties.getQueueWorkerProperties(queueId);
        int workerNum = workerProperties.getWorkerNum();
        for (int i = 0; i < workerNum; ++i) {
          workers.execute(
              new MaestroQueueWorker(
                  String.format("queue-[%s]-worker-[%s]", queueId, i),
                  queueId,
                  workerProperties,
                  queueDao,
                  scheduler,
                  messageQueue,
                  dispatcher,
                  metrics));
        }
      }
    }
  }

  /** Pre-destroy to shut down MaestroQueueSystem. */
  public void shutdown() {
    LOG.info("MaestroQueueWorkerService starts to shutdown ...");
    workers.shutdownNow(); // interrupt all workers
    scheduler.shutdownNow(); // interrupt the scheduler
    LOG.info("MaestroQueueWorkerService shutdown is completed.");
  }
}
