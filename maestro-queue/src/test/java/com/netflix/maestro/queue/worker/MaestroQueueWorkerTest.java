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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.queue.dao.MaestroQueueDao;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.metrics.MetricConstants;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.queue.processors.MaestroJobEventDispatcher;
import com.netflix.maestro.queue.properties.QueueProperties;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroQueueWorkerTest extends MaestroBaseTest {
  @Mock MaestroQueueDao queueDao;
  @Mock ScheduledExecutorService scheduler;
  @Mock BlockingQueue<MessageDto> messageQueue;
  @Mock MaestroJobEventDispatcher dispatcher;
  @Mock MaestroMetrics metrics;

  @Mock MaestroJobEvent jobEvent;
  private MessageDto message;
  private MaestroQueueWorker queueWorker;

  @Before
  public void setup() {
    queueWorker =
        new MaestroQueueWorker(
            "test-worker",
            1,
            new QueueProperties.QueueWorkerProperties(),
            queueDao,
            scheduler,
            messageQueue,
            dispatcher,
            metrics);
    message = new MessageDto(System.currentTimeMillis() + 300000, "msgId", jobEvent, 123456L);
    when(jobEvent.getType()).thenReturn(MaestroJobEvent.Type.START_WORKFLOW);
  }

  @Test
  public void testRun() throws InterruptedException {
    when(messageQueue.take()).thenReturn(message).thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenReturn(Optional.empty());
    queueWorker.run();
    verify(dispatcher, times(1)).processJobEvent(any());
    verify(queueDao, times(1)).remove(message);
    verify(messageQueue, times(1)).drainTo(any(), anyInt());
    verify(queueDao, times(1)).release(anyInt(), anyLong(), any());
  }

  @Test
  public void testRunWithProcessError() throws InterruptedException {
    message = new MessageDto(System.currentTimeMillis() + 9000, "msgId", jobEvent, 123456L);
    when(messageQueue.take()).thenReturn(message).thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenReturn(Optional.of(jobEvent));
    queueWorker.run();
    verify(dispatcher, times(1)).processJobEvent(any());
    verify(queueDao, times(1)).replace(eq(message), eq(jobEvent), anyLong());
    verify(messageQueue, times(1)).drainTo(any(), anyInt());
    verify(queueDao, times(1)).release(anyInt(), anyLong(), any());
  }

  @Test
  public void testRunWithProcessAndPiggybackProcess() throws InterruptedException {
    when(messageQueue.take()).thenReturn(message).thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenReturn(Optional.of(jobEvent));
    queueWorker.run();
    verify(dispatcher, times(2)).processJobEvent(any());
    verify(queueDao, times(0)).replace(eq(message), eq(jobEvent), anyLong());
    verify(queueDao, times(1)).remove(eq(message));
    verify(messageQueue, times(1)).drainTo(any(), anyInt());
    verify(queueDao, times(1)).release(anyInt(), anyLong(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRunWithProcessException() throws InterruptedException {
    when(messageQueue.take()).thenReturn(message).thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenThrow(new RuntimeException("test"));
    queueWorker.run();
    verify(dispatcher, times(1)).processJobEvent(any());
    verify(queueDao, times(0)).remove(message);
    verify(scheduler, times(1)).schedule(any(Callable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    verify(messageQueue, times(1)).drainTo(any(), anyInt());
    verify(queueDao, times(1)).release(anyInt(), anyLong(), any());
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_WORKER_PROCESS_ERROR,
            MaestroQueueWorker.class,
            MetricConstants.RETRYABLE_TAG,
            "true",
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRunWithProcessInternalError() throws InterruptedException {
    when(messageQueue.take()).thenReturn(message).thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenThrow(new MaestroInternalError("test"));
    queueWorker.run();
    verify(dispatcher, times(1)).processJobEvent(any());
    verify(queueDao, times(1)).remove(message);
    verify(scheduler, times(0)).schedule(any(Callable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    verify(messageQueue, times(1)).drainTo(any(), anyInt());
    verify(queueDao, times(1)).release(anyInt(), anyLong(), any());
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_WORKER_PROCESS_ERROR,
            MaestroQueueWorker.class,
            MetricConstants.RETRYABLE_TAG,
            "false",
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRunWithProcessNotFoundException() throws InterruptedException {
    when(messageQueue.take()).thenReturn(message).thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenThrow(new MaestroNotFoundException("test"));
    queueWorker.run();
    verify(dispatcher, times(1)).processJobEvent(any());
    verify(queueDao, times(1)).remove(message);
    verify(scheduler, times(0)).schedule(any(Callable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    verify(messageQueue, times(1)).drainTo(any(), anyInt());
    verify(queueDao, times(1)).release(anyInt(), anyLong(), any());
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_WORKER_PROCESS_ERROR,
            MaestroQueueWorker.class,
            MetricConstants.RETRYABLE_TAG,
            "false",
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }

  @Test
  public void testRunWithInterruption() throws InterruptedException {
    when(messageQueue.take()).thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenReturn(Optional.empty());
    queueWorker.run();
    verify(dispatcher, times(0)).processJobEvent(any());
    verify(queueDao, times(0)).remove(message);
    verify(messageQueue, times(1)).drainTo(any(), anyInt());
    verify(queueDao, times(1)).release(anyInt(), anyLong(), any());
  }

  @Test
  public void testRunForExpiredMessage() throws InterruptedException {
    when(messageQueue.take())
        .thenReturn(new MessageDto(0, "msgId", jobEvent, 123456L))
        .thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenReturn(Optional.empty());
    queueWorker.run();
    verify(dispatcher, times(0)).processJobEvent(any());
    verify(queueDao, times(0)).remove(message);
    verify(messageQueue, times(1)).drainTo(any(), anyInt());
    verify(queueDao, times(1)).release(anyInt(), anyLong(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRunWithInternalMessage() throws InterruptedException {
    when(messageQueue.take())
        .thenReturn(new MessageDto(Long.MAX_VALUE, "msgId", null, 123456L))
        .thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenReturn(Optional.empty());
    when(queueDao.dequeueUnownedMessages(anyInt(), anyLong(), anyInt()))
        .thenReturn(List.of(message));
    queueWorker.run();
    verify(dispatcher, times(0)).processJobEvent(any());
    verify(queueDao, times(0)).remove(message);
    verify(queueDao, times(1)).dequeueUnownedMessages(anyInt(), anyLong(), anyInt());
    verify(messageQueue, times(1)).size();
    verify(messageQueue, times(1)).offer(any());
    verify(scheduler, times(1)).schedule(any(Callable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testRunAndExtendOwnership() throws InterruptedException {
    when(messageQueue.take())
        .thenReturn(new MessageDto(System.currentTimeMillis() + 10000, "msgId", jobEvent, 123456L))
        .thenThrow(new InterruptedException("test"));
    when(dispatcher.processJobEvent(any())).thenThrow(new RuntimeException("test"));
    queueWorker.run();
    verify(dispatcher, times(1)).processJobEvent(any());
    verify(queueDao, times(1)).extendOwnership(any(), anyLong());
    verify(messageQueue, times(1)).drainTo(any(), anyInt());
    verify(queueDao, times(1)).release(anyInt(), anyLong(), any());
    verify(metrics, times(1))
        .counter(
            MetricConstants.QUEUE_WORKER_PROCESS_ERROR,
            MaestroQueueWorker.class,
            MetricConstants.RETRYABLE_TAG,
            "true",
            MetricConstants.TYPE_TAG,
            "START_WORKFLOW");
  }
}
