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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.junit.Before;
import org.junit.Test;

public class InMemoryQueueDaoTest extends MaestroEngineBaseTest {
  private InMemoryQueueDao queueDao;
  private Map<String, ConcurrentLinkedDeque<String>> internalQueue;

  @Before
  public void setUp() {
    internalQueue = new ConcurrentHashMap<>();
    queueDao = new InMemoryQueueDao(internalQueue);
  }

  @Test
  public void testPush() {
    String queueName = "test-queue";
    String id = "abcd-1234-defg-5678";
    queueDao.push(queueName, id, 123);
    assertEquals(1, internalQueue.size());
    assertTrue(internalQueue.containsKey(queueName));
    assertEquals(1, internalQueue.get(queueName).size());
    assertEquals(id, internalQueue.get(queueName).peek());
  }

  @Test
  public void testPushIfNotExists() {
    String queueName = "test-queue";
    String id = "abcd-1234-defg-5678";
    assertTrue(queueDao.pushIfNotExists(queueName, id, 123));
    assertEquals(1, internalQueue.size());
    assertTrue(internalQueue.containsKey(queueName));
    assertEquals(1, internalQueue.get(queueName).size());
    assertEquals(id, internalQueue.get(queueName).peek());

    assertFalse(queueDao.pushIfNotExists(queueName, id, 123));
    assertEquals(1, internalQueue.size());
    assertTrue(internalQueue.containsKey(queueName));
    assertEquals(1, internalQueue.get(queueName).size());
    assertEquals(id, internalQueue.get(queueName).peek());
  }

  @Test
  public void testPop() {
    String queueName = "test-queue";
    String id = "abcd-1234-defg-5678";
    assertEquals(Collections.emptyList(), queueDao.pop(queueName, 2, 100));
    queueDao.pushIfNotExists(queueName, id, 123);
    assertEquals(Collections.singletonList(id), queueDao.pop(queueName, 2, 100));
    assertEquals(Collections.emptyList(), queueDao.pop(queueName, 2, 100));
  }

  @Test
  public void testRemove() {
    String queueName = "test-queue";
    String id = "abcd-1234-defg-5678";
    assertEquals(0, queueDao.getSize(queueName));
    queueDao.pushIfNotExists(queueName, id, 123);
    assertEquals(1, queueDao.getSize(queueName));
    queueDao.remove(queueName, id);
    assertEquals(0, queueDao.getSize(queueName));
  }

  @Test
  public void testGetSize() {
    String queueName = "test-queue";
    String id = "abcd-1234-defg-5678";
    assertEquals(0, queueDao.getSize(queueName));
    queueDao.pushIfNotExists(queueName, id, 123);
    assertEquals(1, queueDao.getSize(queueName));
  }

  @Test
  public void testQueuesDetail() {
    String queueName = "test-queue";
    String id = "abcd-1234-defg-5678";
    queueDao.pushIfNotExists(queueName, id, 123);
    assertEquals(Collections.singletonMap(queueName, 1L), queueDao.queuesDetail());
  }

  @Test
  public void testPostpone() {
    String queueName = "test-queue";
    String id = "abcd-1234-defg-5678";
    assertTrue(queueDao.postpone(queueName, id, 0, 0));
    assertEquals(1, internalQueue.size());
    assertEquals(1, internalQueue.get(queueName).size());
    assertEquals(id, internalQueue.get(queueName).peek());
  }

  @Test
  public void testContainsMessage() {
    String queueName = "test-queue";
    String id = "abcd-1234-defg-5678";
    assertFalse(queueDao.containsMessage(queueName, id));
    assertEquals(1, internalQueue.size());
    assertTrue(internalQueue.get(queueName).isEmpty());

    assertTrue(queueDao.pushIfNotExists(queueName, id, 123));
    assertTrue(queueDao.containsMessage(queueName, id));
    assertEquals(1, internalQueue.size());
    assertEquals(1, internalQueue.get(queueName).size());
    assertEquals(id, internalQueue.get(queueName).peek());
  }
}
