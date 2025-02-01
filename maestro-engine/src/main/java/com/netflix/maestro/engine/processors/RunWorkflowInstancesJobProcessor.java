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

import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.handlers.WorkflowRunner;
import com.netflix.maestro.engine.jobevents.RunWorkflowInstancesJobEvent;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.utils.IdHelper;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Processor to consume {@link RunWorkflowInstancesJobEvent} and kick off internal flow instances
 * for all trigger cases. All workflow instances are launched by this processor.
 */
@Slf4j
@AllArgsConstructor
public class RunWorkflowInstancesJobProcessor
    implements MaestroEventProcessor<RunWorkflowInstancesJobEvent> {
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroFlowDao flowDao;
  private final WorkflowRunner workflowRunner;

  @SuppressWarnings({"PMD.AvoidCatchingNPE"})
  @Override
  public void process(Supplier<RunWorkflowInstancesJobEvent> runWorkflowInstancesJobEventSupplier) {
    RunWorkflowInstancesJobEvent jobEvent = runWorkflowInstancesJobEventSupplier.get();
    String workflowId = jobEvent.getWorkflowId();
    jobEvent
        .getInstanceRunUuids()
        .forEach(
            instanceRunUuid -> {
              try {
                WorkflowInstance instance =
                    instanceDao.getWorkflowInstanceRun(
                        workflowId, instanceRunUuid.getInstanceId(), instanceRunUuid.getRunId());
                if (instance.getStatus() == WorkflowInstance.Status.CREATED) {
                  if (instanceRunUuid.getUuid().equals(instance.getWorkflowUuid())) {
                    if (!flowDao.existFlowWithSameKeys(
                        IdHelper.deriveGroupId(instance), instance.getWorkflowUuid())) {
                      String executionId = runWorkflowInstance(instance);
                      LOG.info(
                          "Run a workflow instance {} with an internal execution_id [{}]",
                          instance.getIdentity(),
                          executionId);
                    } else {
                      LOG.warn(
                          "Workflow instance [{}][{}] has already been executed by flow engine. Skip it for dedup",
                          workflowId,
                          instanceRunUuid);
                    }
                  } else {
                    LOG.warn(
                        "Workflow instance: [{}][{}] in job event does not match DB row uuid [{}] and skip it",
                        workflowId,
                        instanceRunUuid,
                        instance.getWorkflowUuid());
                  }
                } else {
                  LOG.info(
                      "workflow instance: [{}][{}] is not in created (i.e. queued) state and skip it",
                      workflowId,
                      instanceRunUuid);
                }
              } catch (MaestroNotFoundException e) {
                // this is the case that DB is rollback but msg is sent.
                LOG.warn(
                    "Not found workflow instance: [{}][{}] and skip it in job listener",
                    workflowId,
                    instanceRunUuid);
              } catch (NullPointerException e) {
                // not retryable
                LOG.error(
                    "Cannot retry if there is a NullPointerException for workflow [{}]",
                    workflowId);
                throw new MaestroInternalError(e, "Something is null");
              } catch (RuntimeException e) {
                LOG.warn("Retry it as getting a runtime error", e);
                throw new MaestroRetryableError(
                    e, "Failed to run a workflow and will retry to run it.");
              }
            });
  }

  private String runWorkflowInstance(WorkflowInstance instance) {
    if (instance.isFreshRun()) {
      return workflowRunner.start(instance);
    } else {
      return workflowRunner.restart(instance);
    }
  }
}
