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

import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.db.InstanceRunUuid;
import com.netflix.maestro.engine.jobevents.TerminateInstancesJobEvent;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Processor to consume {@link TerminateInstancesJobEvent} and terminate workflow instances for all
 * cases. All workflow instances are terminated by this processor. It is only responsible to add the
 * termination actions to the action table.
 */
@Slf4j
@AllArgsConstructor
public class TerminateInstancesJobProcessor
    implements MaestroEventProcessor<TerminateInstancesJobEvent> {
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceActionDao actionDao;

  @Override
  public void process(Supplier<TerminateInstancesJobEvent> messageSupplier) {
    tryTerminateWorkflowInstances(messageSupplier.get());
  }

  @SuppressWarnings({"PMD.AvoidCatchingNPE"})
  List<InstanceRunUuid> tryTerminateWorkflowInstances(TerminateInstancesJobEvent jobEvent) {
    List<InstanceRunUuid> terminatingWorkflows = new ArrayList<>();
    jobEvent
        .getInstanceRunUuids()
        .forEach(
            instanceRunUuid -> {
              try {
                WorkflowInstance instance =
                    instanceDao.getWorkflowInstanceRun(
                        jobEvent.getWorkflowId(),
                        instanceRunUuid.getInstanceId(),
                        instanceRunUuid.getRunId());
                if (terminateWorkflowInstance(instanceRunUuid, jobEvent, instance)) {
                  terminatingWorkflows.add(instanceRunUuid);
                }
              } catch (MaestroNotFoundException e) {
                // this is the case that DB is rollback but msg is sent.
                LOG.warn(
                    "Not found workflow instance: [{}][{}] and skip it in job listener",
                    jobEvent.getWorkflowId(),
                    instanceRunUuid);
              } catch (NullPointerException e) {
                // not retryable
                LOG.error(
                    "Cannot retry if there is a NullPointerException for workflow {}",
                    jobEvent.getWorkflowId(),
                    e);
                throw new MaestroInternalError(e, "Something is null");
              } catch (MaestroInternalError e) {
                throw e;
              } catch (RuntimeException e) {
                LOG.error("Retry it as getting a runtime error", e);
                throw new MaestroRetryableError(
                    e, "Failed to terminate a workflow and will retry to terminate it.");
              }
            });
    return terminatingWorkflows;
  }

  private boolean terminateWorkflowInstance(
      InstanceRunUuid instanceRunUuid,
      TerminateInstancesJobEvent jobEvent,
      WorkflowInstance instance) {
    if (!instance.getStatus().isTerminal()) {
      Checks.notNull(
          instance.getExecutionId(),
          "workflow instance %s execution_id cannot be null",
          instance.getIdentity());
      if (instance.getWorkflowUuid().equals(instanceRunUuid.getUuid())) {
        actionDao.terminate(
            instance, jobEvent.getUser(), jobEvent.getAction(), jobEvent.getReason());
        return true;
      } else {
        LOG.error(
            "Instance: [{}][{}] in job event does not match DB uuid [{}] and it should never happen",
            jobEvent.getWorkflowId(),
            instanceRunUuid,
            instance.getWorkflowUuid());
        throw new MaestroInternalError(
            "Workflow instance: [%s][%s] in job event does not match DB row uuid [%s]",
            jobEvent.getWorkflowId(), instanceRunUuid, instance.getWorkflowUuid());
      }
    } else {
      LOG.info(
          "workflow instance: [{}][{}] is already in terminal state [{}] and skip it",
          jobEvent.getWorkflowId(),
          instance.getStatus(),
          instanceRunUuid);
    }
    return false;
  }
}
