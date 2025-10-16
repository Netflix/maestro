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
package com.netflix.maestro.queue;

import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.queue.dao.MaestroQueueDao;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.metrics.MetricConstants;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.queue.properties.QueueProperties;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("PMD.LooseCoupling")
public class MaestroQueueSystem {
  private final EnumMap<MaestroJobEvent.Type, BlockingQueue<MessageDto>> eventQueues;
  private final MaestroQueueDao queueDao;
  private final QueueProperties properties;
  private final MaestroMetrics metrics;

  public MaestroQueueSystem(
      EnumMap<MaestroJobEvent.Type, BlockingQueue<MessageDto>> eventQueues,
      MaestroQueueDao queueDao,
      QueueProperties properties,
      MaestroMetrics metrics) {
    LOG.info("MaestroQueueSystem is initialized with the properties {}", properties);
    this.queueDao = queueDao;
    this.eventQueues = eventQueues;
    this.properties = properties;
    this.metrics = metrics;
  }

  /** Pre-destroy to shut down MaestroQueueSystem. */
  public void shutdown() {
    LOG.info("MaestroQueueSystem shutdown is completed.");
  }

  /**
   * Enqueue a message to the database queue table in the same transaction to provide exactly once
   * semantics. If this node already has enough in flight messages in the memory, it will only save
   * the message in the table but not take its ownership immediately. Otherwise, it also claims the
   * ownership and puts the message into the in-memory worker queue.
   *
   * @param conn the db connection
   * @param jobEvent job event
   * @return the message dto, if null, it means the job event is enqueued but not owned by this
   *     worker.
   * @throws SQLException if the database operation fails or there is a duplicate record
   */
  public MessageDto enqueue(Connection conn, MaestroJobEvent jobEvent) throws SQLException {
    long validUntil = System.currentTimeMillis() + getTimeoutForJobType(jobEvent.getType());
    var ret = queueDao.enqueueInTransaction(conn, jobEvent, validUntil);
    metrics.counter(
        MetricConstants.QUEUE_SYSTEM_ENQUEUE_TRANSACTION,
        getClass(),
        MetricConstants.TYPE_TAG,
        String.valueOf(jobEvent.getType()));
    return ret;
  }

  /**
   * Enqueue a job event for the cases, there is no need for a transaction. It requires the caller
   * to retry if there is an error.
   *
   * @param jobEvent the job event to enqueue
   * @return an optional details object if there was an error during enqueueing
   */
  public Optional<Details> enqueue(MaestroJobEvent jobEvent) {
    try {
      enqueueOrThrow(jobEvent);
      return Optional.empty();
    } catch (RuntimeException e) {
      metrics.counter(
          MetricConstants.QUEUE_SYSTEM_ENQUEUE_ERROR,
          getClass(),
          MetricConstants.TYPE_TAG,
          String.valueOf(jobEvent.getType()));
      return Optional.of(Details.create(e, true, "Failed to publish a Maestro job event"));
    }
  }

  /**
   * Enqueue a job event for the cases, there is no need for a transaction. If failed, it throws an
   * exception. It requires the caller to retry if there is an error.
   *
   * @param jobEvent job event to enqueue
   */
  public void enqueueOrThrow(MaestroJobEvent jobEvent) {
    long validUntil = System.currentTimeMillis() + getTimeoutForJobType(jobEvent.getType());
    MessageDto message = queueDao.enqueue(jobEvent, validUntil);
    notify(message);
    metrics.counter(
        MetricConstants.QUEUE_SYSTEM_ENQUEUE,
        getClass(),
        MetricConstants.TYPE_TAG,
        String.valueOf(jobEvent.getType()));
  }

  private long getTimeoutForJobType(MaestroJobEvent.Type type) {
    var queue = eventQueues.get(type);
    Integer queueId = type.getQueueId();
    if (queue != null
        && queue.size() < properties.getQueueWorkerProperties(queueId).getMessageLimit()) {
      return properties.getQueueWorkerProperties(queueId).getOwnershipTimeout();
    }
    return -1; // enqueue but don't own it
  }

  /**
   * Notify the queue system that a message is available for processing. It will be added to the
   * shared queue of the workers.
   *
   * @param message the message to notify
   */
  public void notify(MessageDto message) {
    if (message == null) {
      return;
    }
    if (message.ownedUntil() < System.currentTimeMillis()) {
      LOG.info("not notify as message [{}] is expired and will be ignored", message);
      return;
    }
    var queue = eventQueues.get(message.type());
    if (queue != null) {
      queue.offer(message);
      metrics.counter(
          MetricConstants.QUEUE_SYSTEM_NOTIFY,
          getClass(),
          MetricConstants.TYPE_TAG,
          String.valueOf(message.type()));
    } else {
      metrics.counter(
          MetricConstants.QUEUE_SYSTEM_NOTIFY_ERROR,
          getClass(),
          MetricConstants.TYPE_TAG,
          String.valueOf(message.type()));
      throw new MaestroInternalError("MaestroQueueSystem does not support message [%s]", message);
    }
  }
}
