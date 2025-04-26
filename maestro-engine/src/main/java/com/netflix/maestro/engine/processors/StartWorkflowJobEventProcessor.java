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
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.handlers.WorkflowRunner;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.queue.models.InstanceRunUuid;
import com.netflix.maestro.queue.processors.MaestroEventProcessor;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Processor to consume {@link StartWorkflowJobEvent} and it considers run strategy including
 * concurrency limit. Once it decided workflow instances to run, it sends RunWorkflowInstancesJob
 * event with the configured batch size.
 */
@Slf4j
@AllArgsConstructor
public class StartWorkflowJobEventProcessor
    implements MaestroEventProcessor<StartWorkflowJobEvent> {
  private final MaestroWorkflowDao workflowDao;
  private final MaestroRunStrategyDao runStrategyDao;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final WorkflowRunner workflowRunner;

  @Override
  public Optional<MaestroJobEvent> process(StartWorkflowJobEvent jobEvent) {
    try {
      startWorkflowInstance(jobEvent.getWorkflowId());
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
    return Optional.empty();
  }

  @VisibleForTesting
  void startWorkflowInstance(String workflowId) {
    RunStrategy runStrategy = workflowDao.getRunStrategy(workflowId);
    List<InstanceRunUuid> runInstances =
        runStrategyDao.dequeueWithRunStrategy(workflowId, runStrategy);

    int size = runInstances == null ? 0 : runInstances.size();
    if (size > 0) {
      runInstances.forEach(
          instanceRunUuid -> {
            WorkflowInstance instance =
                instanceDao.getWorkflowInstanceRun(
                    workflowId, instanceRunUuid.getInstanceId(), instanceRunUuid.getRunId());
            workflowRunner.run(instance, instanceRunUuid.getUuid());
          });
    }
    LOG.info("Run [{}] dequeued workflow instance(s) for workflow [{}]", size, workflowId);

    if (size >= Constants.DEQUEUE_SIZE_LIMIT) {
      LOG.debug(
          "Hit DEQUEUE_SIZE_LIMIT ({}) for workflow [{}] with concurrency [{}], sending another start job event",
          size,
          workflowId,
          runStrategy.getWorkflowConcurrency());
      throw new MaestroRetryableError(
          "Hit DEQUEUE_SIZE_LIMIT for workflow [%s], retrying the job", workflowId);
    }
  }
}
