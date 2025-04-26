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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.queue.dao.MaestroQueueDao;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.metrics.MetricConstants;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.queue.properties.QueueProperties;
import java.util.EnumMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroQueueSystemTest extends MaestroBaseTest {
  @Mock private EnumMap<MaestroJobEvent.Type, BlockingQueue<MessageDto>> eventQueues;
  @Mock private MaestroQueueDao queueDao;
  @Mock private MaestroMetrics metrics;

  @Mock private MaestroJobEvent jobEvent;
  private MessageDto message;

  private MaestroQueueSystem queueSystem;

  @Before
  public void setUp() {
    queueSystem = new MaestroQueueSystem(eventQueues, queueDao, new QueueProperties(), metrics);
    message = new MessageDto(System.currentTimeMillis() + 300000, "msgId", jobEvent, 123456L);
    when(jobEvent.getType()).thenReturn(MaestroJobEvent.Type.START_WORKFLOW);
  }

  @Test
  public void testEnqueue() throws Exception {
    when(queueDao.enqueueInTransaction(any(), any(), anyLong())).thenReturn(message);
    var actual = queueSystem.enqueue(null, jobEvent);

    assertEquals(message, actual);
    verify(eventQueues, times(1)).get(eq(MaestroJobEvent.Type.START_WORKFLOW));
    verify(queueDao, times(1)).enqueueInTransaction(any(), any(), anyLong());
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_SYSTEM_ENQUEUE_TRANSACTION,
            MaestroQueueSystem.class,
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }

  @Test
  public void testEnqueueWithoutConnection() {
    when(queueDao.enqueue(any(), anyLong())).thenReturn(message);
    BlockingQueue<MessageDto> queue = new LinkedBlockingQueue<>();
    when(eventQueues.get(MaestroJobEvent.Type.START_WORKFLOW)).thenReturn(queue);

    var res = queueSystem.enqueue(jobEvent);
    assertTrue(res.isEmpty());
    verify(eventQueues, times(2)).get(eq(MaestroJobEvent.Type.START_WORKFLOW));
    verify(queueDao, times(1)).enqueue(any(), anyLong());
    assertEquals(1, queue.size());
    assertEquals(message, queue.poll());
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_SYSTEM_ENQUEUE,
            MaestroQueueSystem.class,
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }

  @Test
  public void testEnqueueWithoutConnectionWithError() {
    when(queueDao.enqueue(any(), anyLong())).thenThrow(new RuntimeException("test"));
    var res = queueSystem.enqueue(jobEvent);
    assertTrue(res.isPresent());
    assertEquals("Failed to publish a Maestro job event", res.get().getMessage());
    verify(eventQueues, times(1)).get(eq(MaestroJobEvent.Type.START_WORKFLOW));
    verify(queueDao, times(1)).enqueue(any(), anyLong());
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_SYSTEM_ENQUEUE_ERROR,
            MaestroQueueSystem.class,
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }

  @Test
  public void testEnqueueOrThrow() {
    when(queueDao.enqueue(any(), anyLong())).thenReturn(message);
    BlockingQueue<MessageDto> queue = new LinkedBlockingQueue<>();
    when(eventQueues.get(MaestroJobEvent.Type.START_WORKFLOW)).thenReturn(queue);

    queueSystem.enqueueOrThrow(jobEvent);

    verify(eventQueues, times(2)).get(eq(MaestroJobEvent.Type.START_WORKFLOW));
    verify(queueDao, times(1)).enqueue(any(), anyLong());
    assertEquals(1, queue.size());
    assertEquals(message, queue.poll());
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_SYSTEM_ENQUEUE,
            MaestroQueueSystem.class,
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }

  @Test
  public void testEnqueueOrThrowWithError() {
    when(queueDao.enqueue(any(), anyLong())).thenThrow(new RuntimeException("test"));
    AssertHelper.assertThrows(
        "should throw", RuntimeException.class, "test", () -> queueSystem.enqueueOrThrow(jobEvent));
    verify(eventQueues, times(1)).get(eq(MaestroJobEvent.Type.START_WORKFLOW));
    verify(queueDao, times(1)).enqueue(any(), anyLong());
  }

  @Test
  public void testNotify() {
    queueSystem.notify(null);
    verify(eventQueues, times(0)).get(eq(MaestroJobEvent.Type.START_WORKFLOW));

    BlockingQueue<MessageDto> queue = new LinkedBlockingQueue<>();
    when(eventQueues.get(MaestroJobEvent.Type.START_WORKFLOW)).thenReturn(queue);
    queueSystem.notify(message);
    verify(eventQueues, times(1)).get(eq(MaestroJobEvent.Type.START_WORKFLOW));
    assertEquals(1, queue.size());
    assertEquals(message, queue.poll());
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_SYSTEM_NOTIFY,
            MaestroQueueSystem.class,
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }

  @Test
  public void testNotifyWithError() {
    AssertHelper.assertThrows(
        "should throw if no queue",
        MaestroInternalError.class,
        "MaestroQueueSystem does not support message",
        () -> queueSystem.notify(message));
    verify(eventQueues, times(1)).get(eq(MaestroJobEvent.Type.START_WORKFLOW));
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_SYSTEM_NOTIFY_ERROR,
            MaestroQueueSystem.class,
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }
}
