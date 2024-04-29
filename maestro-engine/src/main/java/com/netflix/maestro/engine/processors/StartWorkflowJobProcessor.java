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

import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.definition.RunStrategy;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Processor to consume {@link StartWorkflowJobEvent} and it considers run strategy including
 * concurrency limit. Once it decided workflow instances to run, it sends RunWorkflowInstancesJob
 * event with the configured batch size.
 */
@Slf4j
@AllArgsConstructor
public class StartWorkflowJobProcessor implements MaestroEventProcessor<StartWorkflowJobEvent> {
  private final MaestroWorkflowDao workflowDao;
  private final MaestroRunStrategyDao runStrategyDao;

  @Override
  public void process(Supplier<StartWorkflowJobEvent> messageSupplier) {
    try {
      StartWorkflowJobEvent startWorkflowJobEvent = messageSupplier.get();
      startWorkflowInstance(startWorkflowJobEvent.getWorkflowId());
    } catch (MaestroNotFoundException e) {
      LOG.error("Cannot retry as this is a non-retryable error and the start job is removed.", e);
      // then the start job is treated as processed and removed.
    } catch (MaestroRetryableError e) {
      LOG.error("Retry to start it as getting a retryable error", e);
      throw e;
    } catch (RuntimeException e) {
      LOG.error("Retry it as getting a runtime error", e);
      throw new MaestroRetryableError(
          e, "Failed to start a workflow instance and will retry to run it.");
    }
  }

  @VisibleForTesting
  void startWorkflowInstance(String workflowId) {
    RunStrategy runStrategy = workflowDao.getRunStrategy(workflowId);
    int res = runStrategyDao.dequeueWithRunStrategy(workflowId, runStrategy);
    LOG.info("Run [{}] dequeued workflow instance(s) for workflow {}", res, workflowId);
  }
}
