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
package com.netflix.maestro.engine.dao;

import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory implementation of QueueDAO for local testing purpose. It is expected to be thread safe
 * but does not have a transactional support. It also ignores the priority, offsetTimeInSecond,
 * postponeDurationInSeconds, etc.
 */
@Slf4j
@AllArgsConstructor
public class InMemoryQueueDao implements QueueDAO {
  private final Map<String, ConcurrentLinkedDeque<String>> queues;

  @Override
  public void push(String queueName, String id, long offsetTimeInSecond) {
    push(queueName, id, 0, offsetTimeInSecond);
  }

  @Override
  public void push(String queueName, String id, int priority, long offsetTimeInSecond) {
    if (!queues.containsKey(queueName)) {
      queues.put(queueName, new ConcurrentLinkedDeque<>());
    }
    LOG.info("push a task [{}] to queue [{}]", id, queueName);
    queues.get(queueName).offerLast(id);
  }

  @Override
  public void push(String queueName, List<Message> messages) {}

  @Override
  public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
    return pushIfNotExists(queueName, id, 0, offsetTimeInSecond);
  }

  @Override
  public boolean pushIfNotExists(
      String queueName, String id, int priority, long offsetTimeInSecond) {
    if (!containsMessage(queueName, id)) {
      push(queueName, id, priority, offsetTimeInSecond);
      return true;
    }
    return false;
  }

  @Override
  public List<String> pop(String queueName, int count, int timeout) {
    List<String> tasks = new ArrayList<>(count);
    if (!queues.containsKey(queueName)) {
      queues.put(queueName, new ConcurrentLinkedDeque<>());
    }
    for (int i = 0; i < count; ++i) {
      String entry = queues.get(queueName).pollFirst();
      if (entry != null) {
        tasks.add(entry);
      } else {
        break;
      }
    }
    if (!tasks.isEmpty()) {
      LOG.info("pop messages [{}] for queue [{}]", tasks, queueName);
    }
    return tasks;
  }

  @Override
  public List<Message> pollMessages(String queueName, int count, int timeout) {
    return null;
  }

  @Override
  public void remove(String queueName, String messageId) {
    if (!queues.containsKey(queueName)) {
      queues.put(queueName, new ConcurrentLinkedDeque<>());
    }
    queues.get(queueName).remove(messageId);
  }

  @Override
  public int getSize(String queueName) {
    if (!queues.containsKey(queueName)) {
      queues.put(queueName, new ConcurrentLinkedDeque<>());
    }
    return queues.get(queueName).size();
  }

  @Override
  public boolean ack(String queueName, String messageId) {
    return false;
  }

  @Override
  public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
    return false;
  }

  @Override
  public void flush(String queueName) {}

  @Override
  public Map<String, Long> queuesDetail() {
    Map<String, Long> detail = new HashMap<>();
    queues.forEach((k, v) -> detail.put(k, (long) v.size()));
    return detail;
  }

  @Override
  public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
    return null;
  }

  @Override
  public boolean resetOffsetTime(String queueName, String id) {
    return postpone(queueName, id, 0, 0);
  }

  @Override
  public List<String> pop(String queueName, int count, int timeout, long leaseDurationSeconds) {
    return null;
  }

  @Override
  public List<Message> pollMessages(
      String queueName, int count, int timeout, long leaseDurationSeconds) {
    return null;
  }

  @Override
  public void processUnacks(String queueName) {}

  @Override
  public boolean postpone(
      String queueName, String messageId, int priority, long postponeDurationInSeconds) {
    remove(queueName, messageId);
    push(queueName, messageId, priority, postponeDurationInSeconds);
    return true;
  }

  @Override
  public boolean containsMessage(String queueName, String messageId) {
    if (!queues.containsKey(queueName)) {
      queues.put(queueName, new ConcurrentLinkedDeque<>());
    }
    return queues.get(queueName).contains(messageId);
  }
}
