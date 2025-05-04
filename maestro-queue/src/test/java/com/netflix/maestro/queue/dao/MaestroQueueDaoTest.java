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
package com.netflix.maestro.queue.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.MaestroDatabaseHelper;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.queue.jobevents.NotificationJobEvent;
import com.netflix.maestro.queue.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.queue.models.MessageDto;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroQueueDaoTest extends MaestroBaseTest {
  private final DatabaseConfiguration config = MaestroDatabaseHelper.getConfig();
  private final DataSource dataSource = MaestroDatabaseHelper.getDataSource();

  @Mock private MaestroMetrics metrics;

  private MaestroQueueDao dao;
  private StartWorkflowJobEvent jobEvent;

  @Before
  public void setUp() {
    dao = new MaestroQueueDao(dataSource, MAPPER, config, metrics);
    jobEvent = new StartWorkflowJobEvent();
    jobEvent.setWorkflowId("test-wf");
  }

  @Test
  public void testEnqueueInTransaction() throws Exception {
    Connection conn = dataSource.getConnection();
    MessageDto actual = dao.enqueueInTransaction(conn, jobEvent, 123456789L);
    conn.commit();
    conn.close();
    assertEquals(123456789L, actual.ownedUntil());
    assertEquals(jobEvent, actual.event());
    dao.remove(actual);
  }

  @Test
  public void testEnqueue() {
    MessageDto actual = dao.enqueue(jobEvent, 123456789L);
    assertEquals(123456789L, actual.ownedUntil());
    assertEquals(jobEvent, actual.event());
    dao.remove(actual);
  }

  @Test
  public void testRemove() {
    MessageDto enqueued = dao.enqueue(jobEvent, 123456789L);
    dao.remove(enqueued);
    List<MessageDto> owned = dao.dequeueUnownedMessages(jobEvent.getType().getQueueId(), 12345L, 1);
    assertTrue(owned.isEmpty());
  }

  @Test
  public void testExtendOwnership() {
    MessageDto enqueued = dao.enqueue(jobEvent, 123456789L);
    MessageDto extended = dao.extendOwnership(enqueued, 223456789L);
    assertEquals(223456789L, extended.ownedUntil());
    dao.remove(extended);
  }

  @Test
  public void testReplace() {
    NotificationJobEvent jobEvent2 = new NotificationJobEvent();
    MessageDto enqueued = dao.enqueue(jobEvent, 123456789L);
    MessageDto replaced = dao.replace(enqueued, jobEvent2, 223456789L);
    List<MessageDto> owned =
        dao.dequeueUnownedMessages(jobEvent.getType().getQueueId(), 123456789L, 1);
    assertTrue(owned.isEmpty());
    owned = dao.dequeueUnownedMessages(jobEvent2.getType().getQueueId(), 223456789L, 1);
    assertEquals(1, owned.size());
    assertEquals(replaced.msgId(), owned.getFirst().msgId());
    assertTrue(owned.getFirst().ownedUntil() > System.currentTimeMillis());
    assertEquals(replaced.event(), owned.getFirst().event());
    dao.remove(replaced);
  }

  @Test
  public void testDequeueUnownedMessage() {
    MessageDto enqueued = dao.enqueue(jobEvent, 12345L);
    List<MessageDto> owned = dao.dequeueUnownedMessages(jobEvent.getType().getQueueId(), 12345L, 1);
    assertEquals(1, owned.size());
    assertEquals(enqueued.msgId(), owned.getFirst().msgId());
    assertTrue(owned.getFirst().ownedUntil() > System.currentTimeMillis());
    assertEquals(enqueued.event(), owned.getFirst().event());
    assertEquals(enqueued.createTime(), owned.getFirst().createTime());
    dao.remove(owned.getFirst());
  }

  @Test
  public void testRelease() {
    StartWorkflowJobEvent jobEvent2 = new StartWorkflowJobEvent();
    var msgs = List.of(dao.enqueue(jobEvent, 123456789L), dao.enqueue(jobEvent2, 123456789L));
    Optional<Details> errors = dao.release(jobEvent.getType().getQueueId(), 12345L, msgs);
    assertTrue(errors.isEmpty());
    List<MessageDto> owned =
        dao.dequeueUnownedMessages(jobEvent.getType().getQueueId(), 123456789L, 10);
    assertEquals(2, owned.size());
    assertEquals(
        msgs.stream().map(MessageDto::msgId).collect(Collectors.toSet()),
        owned.stream().map(MessageDto::msgId).collect(Collectors.toSet()));
    dao.remove(owned.getFirst());
    dao.remove(owned.get(1));
  }
}
