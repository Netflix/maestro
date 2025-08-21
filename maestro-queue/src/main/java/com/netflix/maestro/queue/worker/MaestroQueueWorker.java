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

import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.queue.dao.MaestroQueueDao;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.metrics.MetricConstants;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.queue.processors.MaestroJobEventDispatcher;
import com.netflix.maestro.queue.properties.QueueProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MaestroQueueWorker implements Runnable {
  private static final long JITTER_IN_MILLIS = 1000;

  private final String name;
  private final int queueId;
  private final int batchLimit;
  private final int messageLimit;
  private final long timeout;
  private final long executionBudget;
  private final long scanInterval;
  private final long retryInterval;

  private final MaestroQueueDao queueDao;
  private final ScheduledExecutorService scheduler;
  private final BlockingQueue<MessageDto> messageQueue;
  private final MaestroJobEventDispatcher dispatcher;
  private final MaestroMetrics metrics;

  private volatile boolean running = true;

  public MaestroQueueWorker(
      String name,
      int queueId,
      QueueProperties.QueueWorkerProperties properties,
      MaestroQueueDao queueDao,
      ScheduledExecutorService scheduler,
      BlockingQueue<MessageDto> messageQueue,
      MaestroJobEventDispatcher dispatcher,
      MaestroMetrics metrics) {
    this.name = name;
    this.queueId = queueId;
    this.batchLimit = properties.getBatchLimit();
    this.messageLimit = properties.getMessageLimit();
    this.timeout = properties.getOwnershipTimeout();
    this.executionBudget = properties.getExecutionBudget();
    this.scanInterval = properties.getScanInterval();
    this.retryInterval = properties.getRetryInterval();
    this.queueDao = queueDao;
    this.scheduler = scheduler;
    this.messageQueue = messageQueue;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
  }

  @Override
  public void run() {
    LOG.info("[{}] is ready to run", name);
    while (running) {
      MessageDto message = dequeueMessage();
      if (message == null) { // interrupted
        continue;
      }
      try {
        if (running) { // double check to avoid extra run but no guarantee
          processMessage(message);
        }
      } catch (MaestroNotFoundException | MaestroInternalError m) {
        // ignore the internal errored message
        LOG.warn(
            "[{}] got an non-retryable error for message [{}] and ignore it", name, message, m);
        metrics.counter(
            MetricConstants.QUEUE_WORKER_PROCESS_ERROR,
            getClass(),
            MetricConstants.RETRYABLE_TAG,
            "false",
            MetricConstants.TYPE_TAG,
            String.valueOf(message.type()));
      } catch (RuntimeException e) {
        LOG.warn(
            "[{}] got an exception for message [{}] and will retry the message", name, message, e);
        requeue(message, retryInterval); // requeue and retry the message after some interval
        metrics.counter(
            MetricConstants.QUEUE_WORKER_PROCESS_ERROR,
            getClass(),
            MetricConstants.RETRYABLE_TAG,
            "true",
            MetricConstants.TYPE_TAG,
            String.valueOf(message.type()));
      }
    }
    releaseOwnership();
    LOG.info("[{}] is not running any more.", name);
  }

  private void releaseOwnership() {
    int res = batchLimit;
    int total = 0;
    while (res > 0) {
      List<MessageDto> messages = new ArrayList<>();
      res = messageQueue.drainTo(messages, batchLimit);
      var events = messages.stream().filter(m -> !m.inMemory()).toList();
      LOG.info("releasing [{}]events/[{}]messages for queue_id [{}]", events.size(), res, queueId);
      total += res;
      var failed = queueDao.release(queueId, System.currentTimeMillis(), events);
      failed.ifPresent(
          details -> LOG.info("Failed to release messages for [{}] due to [{}]", name, details));
    }
    LOG.info("Totally released [{}] messages for queue_id [{}]", total, queueId);
  }

  private void processMessage(MessageDto message) {
    final long curTime = System.currentTimeMillis();
    if (message.isInternal()) {
      int currentSize = messageQueue.size();
      metrics.gauge(
          MetricConstants.WORKER_QUEUE_SIZE,
          currentSize,
          getClass(),
          MetricConstants.TYPE_TAG,
          String.valueOf(queueId));
      if (currentSize < messageLimit) {
        var messages =
            queueDao.dequeueUnownedMessages(
                queueId, timeout, Math.min(batchLimit, messageLimit - currentSize));
        messages.forEach(messageQueue::offer);
        if (!messages.isEmpty()) {
          LOG.info(
              "enqueued [{}] messages for [{}] with existing queue size [{}]",
              messages.size(),
              name,
              currentSize);
        }
      } else {
        LOG.info(
            "skip dequeue messages for [{}] as the queue [{}] is over the size limit [{}]",
            name,
            currentSize,
            messageLimit);
      }
      requeue(message, scanInterval + (int) (JITTER_IN_MILLIS * Math.random()));
    } else if (message.ownedUntil() > curTime) {
      metrics.timer(
          MetricConstants.WORKER_QUEUE_QUEUEING_DELAY,
          curTime - message.createTime(),
          getClass(),
          MetricConstants.TYPE_TAG,
          String.valueOf(queueId));
      LOG.debug(
          "taking action for [{}] with a queueing delay [{}]ms",
          name,
          curTime - message.createTime());
      Optional<MaestroJobEvent> next = dispatcher.processJobEvent(message.event());
      if (next.isPresent()) {
        processOrReplace(message, next.get(), curTime);
      } else {
        queueDao.remove(message);
      }
    } else {
      LOG.warn("ownership of message [{}] has expired for the worker [{}]", message, name);
    }
  }

  private void processOrReplace(MessageDto prevMsg, MaestroJobEvent jobEvent, long curTime) {
    try {
      if (prevMsg.ownedUntil() > curTime + executionBudget) {
        LOG.debug(
            "Piggyback processing the next job event [{}] of msg [{}] in worker [{}]",
            jobEvent,
            prevMsg.msgId(),
            name);
        dispatcher.processJobEvent(jobEvent);
        queueDao.remove(prevMsg);
        return;
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to piggyback processing the next job event [{}] of msg [{}] in worker [{}], default to replacing",
          jobEvent,
          prevMsg.msgId(),
          name,
          e);
    }
    // replace the message by the new one and also set the ownedUntil by the current setup
    queueDao.replace(prevMsg, jobEvent, curTime);
    // note that not requeue as the message goes to another queue in the current use cases
  }

  private void requeue(MessageDto message, long delayInMillis) {
    if (!running) {
      LOG.debug(
          "skip posting job event as either [{}] is not running or the message [{}] is in the queue",
          name,
          message);
      return;
    }
    if (delayInMillis <= 0) {
      messageQueue.offer(message);
    } else {
      LOG.debug(
          "enqueue a message [{}] for [{}] with a delay [{}]ms", message, name, delayInMillis);
      // have to extend the message ownership before retrying
      var extended = extendOwnershipIfNeeded(message);
      if (extended != null) {
        scheduler.schedule(
            () -> messageQueue.offer(extended), delayInMillis, TimeUnit.MILLISECONDS);
      }
    }
  }

  private MessageDto extendOwnershipIfNeeded(MessageDto message) {
    long curTime = System.currentTimeMillis();
    if (message.isInternal() || message.ownedUntil() > curTime + executionBudget) {
      return message;
    } else {
      return queueDao.extendOwnership(message, curTime + timeout + retryInterval);
    }
  }

  private MessageDto dequeueMessage() {
    try {
      MessageDto message = messageQueue.take();
      LOG.debug("dequeued a job message [{}] for [{}]", message, name);
      return message;
    } catch (InterruptedException e) {
      LOG.warn(
          "[{}] is interrupted, running flag value for [{}] is [{}]",
          Thread.currentThread(),
          name,
          running);
      terminateNow();
      return null;
    }
  }

  private void terminateNow() {
    running = false;
  }
}
