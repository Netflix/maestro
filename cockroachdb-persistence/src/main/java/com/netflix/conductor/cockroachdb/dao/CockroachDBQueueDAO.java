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
package com.netflix.conductor.cockroachdb.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.cockroachdb.CockroachDBConfiguration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 * Dummy CockroachDB implementation of QueueDAO.
 *
 * @author jun-he
 */
public class CockroachDBQueueDAO extends CockroachDBBaseDAO implements QueueDAO {
  public CockroachDBQueueDAO(
      DataSource dataSource, ObjectMapper objectMapper, CockroachDBConfiguration config) {
    super(dataSource, objectMapper, config);
  }

  @Override
  public void push(String queueName, String id, long offsetTimeInSecond) {}

  @Override
  public void push(String queueName, String id, int priority, long offsetTimeInSecond) {}

  @Override
  public void push(String queueName, List<Message> messages) {}

  @Override
  public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
    return false;
  }

  @Override
  public boolean pushIfNotExists(
      String queueName, String id, int priority, long offsetTimeInSecond) {
    return false;
  }

  @Override
  public List<String> pop(String queueName, int count, int timeout) {
    return null;
  }

  @Override
  public List<Message> pollMessages(String queueName, int count, int timeout) {
    return null;
  }

  @Override
  public void remove(String queueName, String messageId) {}

  @Override
  public int getSize(String queueName) {
    return 0;
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
    return null;
  }

  @Override
  public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
    return null;
  }

  @Override
  public boolean resetOffsetTime(String queueName, String id) {
    return false;
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
    return false;
  }
}
