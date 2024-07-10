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
package com.netflix.maestro.engine.listeners;

import com.netflix.maestro.engine.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import com.netflix.maestro.engine.jobevents.RunWorkflowInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.TerminateInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.TerminateThenRunInstanceJobEvent;
import com.netflix.maestro.engine.processors.DeleteWorkflowJobProcessor;
import com.netflix.maestro.engine.processors.PublishJobEventProcessor;
import com.netflix.maestro.engine.processors.RunWorkflowInstancesJobProcessor;
import com.netflix.maestro.engine.processors.StartWorkflowJobProcessor;
import com.netflix.maestro.engine.processors.TerminateInstancesJobProcessor;
import com.netflix.maestro.engine.processors.TerminateThenRunInstanceJobProcessor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory job event listener to process maestro job events in the queue using job processors. It
 * is for the demo purpose, used together with InMemoryMaestroJobEventPublisher.
 */
@SuppressWarnings({"PMD.DoNotUseThreads"})
@Slf4j
@RequiredArgsConstructor
public class InMemoryJobEventListener {
  private final DeleteWorkflowJobProcessor deleteWorkflowJobProcessor;
  private final RunWorkflowInstancesJobProcessor runWorkflowInstancesJobProcessor;
  private final StartWorkflowJobProcessor startWorkflowJobProcessor;
  private final TerminateInstancesJobProcessor terminateInstancesJobProcessor;
  private final TerminateThenRunInstanceJobProcessor terminateThenRunInstanceJobProcessor;
  private final PublishJobEventProcessor publishJobEventProcessor;
  private final LinkedBlockingQueue<MaestroJobEvent> queue;
  private final ExecutorService executorService;

  public void postConstruct() {
    executorService.execute(
        () -> {
          while (true) {
            try {
              process(queue.take());
            } catch (InterruptedException e) {
              break;
            } catch (RuntimeException e) {
              LOG.error("Failed to process a maestro job event and discard it");
            }
          }
        });
  }

  public void preDestroy() {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.info("executor shutdown is timed out and calling shutdownNow");
        executorService.shutdownNow();
      }
    } catch (InterruptedException ex) {
      LOG.info("executor shutdown is interrupted and calling shutdownNow");
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Process a maestro job event locally for the demo purpose.
   *
   * @param maestroJob maestro job event to process
   */
  void process(MaestroJobEvent maestroJob) {
    LOG.info("process a maestro job event: [{}]", maestroJob);
    switch (maestroJob.getType()) {
      case DELETE_WORKFLOW_JOB_EVENT:
        deleteWorkflowJobProcessor.process(() -> (DeleteWorkflowJobEvent) maestroJob);
        break;
      case RUN_WORKFLOW_INSTANCES_JOB_EVENT:
        runWorkflowInstancesJobProcessor.process(() -> (RunWorkflowInstancesJobEvent) maestroJob);
        break;
      case START_WORKFLOW_JOB_EVENT:
        startWorkflowJobProcessor.process(() -> (StartWorkflowJobEvent) maestroJob);
        break;
      case TERMINATE_INSTANCES_JOB_EVENT:
        terminateInstancesJobProcessor.process(() -> (TerminateInstancesJobEvent) maestroJob);
        break;
      case TERMINATE_THEN_RUN_JOB_EVENT:
        terminateThenRunInstanceJobProcessor.process(
            () -> (TerminateThenRunInstanceJobEvent) maestroJob);
        break;
      default:
        publishJobEventProcessor.process(() -> maestroJob);
        break;
    }
  }
}
