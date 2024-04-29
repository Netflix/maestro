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
package com.netflix.maestro.engine.processors;

import com.netflix.maestro.engine.dao.MaestroWorkflowDeletionDao;
import com.netflix.maestro.engine.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.events.MaestroEvent;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Processor to consume {@link DeleteWorkflowJobEvent}. As it might take unknown time to finish,
 * this can be a long-running job. In most of the cases, it can be done with message visibility
 * timeout (e.g. sqs). But if it is timed out, then there are chances multiple processors running to
 * delete the same workflow. It should still be fine. To reduce the transaction conflict, also add a
 * timeout within the processor code to terminate itself if running too long.
 */
@Slf4j
@AllArgsConstructor
public class DeleteWorkflowJobProcessor implements MaestroEventProcessor<DeleteWorkflowJobEvent> {
  private static final long TIME_OUT_IN_NANOS = TimeUnit.SECONDS.toNanos(117); // 2-min minus 3-sec

  private final MaestroWorkflowDeletionDao deletionDao;
  private final MaestroNotificationPublisher eventClient;
  private final String clusterName;

  @Override
  public void process(Supplier<DeleteWorkflowJobEvent> messageSupplier) {
    try {
      DeleteWorkflowJobEvent deletionEvent = messageSupplier.get();
      if (deletionDao.isDeletionInitialized(
          deletionEvent.getWorkflowId(), deletionEvent.getInternalId())) {
        MaestroEvent event = deletionEvent.toMaestroEvent(clusterName);
        LOG.info("Send out external deletion event [{}] to downstream services ", event);
        eventClient.send(event);
      }
      deletionDao.deleteWorkflowData(
          deletionEvent.getWorkflowId(), deletionEvent.getInternalId(), TIME_OUT_IN_NANOS);
    } catch (MaestroNotFoundException e) {
      LOG.error(
          "Cannot retry as this is a non-retryable error and the deletion job is removed.", e);
      // then the deletion job is treated as processed and removed.
    } catch (MaestroRetryableError e) {
      LOG.error("Retry to delete it as getting a retryable error", e);
      throw e;
    } catch (RuntimeException e) {
      LOG.error("Retry to delete it as getting a runtime error", e);
      throw new MaestroRetryableError(
          e, "Failed to delete a workflow and will retry the deletion.");
    }
  }
}
