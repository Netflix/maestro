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
import com.netflix.maestro.engine.handlers.WorkflowRunner;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.jobevents.TerminateThenRunJobEvent;
import com.netflix.maestro.queue.models.InstanceRunUuid;
import com.netflix.maestro.queue.processors.MaestroEventProcessor;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Processor to consume {@link TerminateThenRunJobEvent} and terminate maestro workflow instances
 * for all cases. It monitors until workflow instances are terminated eventually. Then it runs a new
 * workflow instance directly.
 */
@Slf4j
@AllArgsConstructor
public class TerminateThenRunJobEventProcessor
    implements MaestroEventProcessor<TerminateThenRunJobEvent> {
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceActionDao actionDao;
  private final WorkflowRunner workflowRunner;

  @Override
  public Optional<MaestroJobEvent> process(TerminateThenRunJobEvent jobEvent) {
    List<InstanceRunUuid> terminated = tryTerminateWorkflowInstances(jobEvent);

    // then monitor the termination for workflows marked as terminated
    checkProgress(jobEvent.getWorkflowId(), terminated);
    // update the event in case that the run fails
    jobEvent.getInstanceRunUuids().clear();
    // after that, run the workflow instances directly without enqueueing a job
    var instanceRunUuids = jobEvent.getRunAfter();
    if (instanceRunUuids != null) {
      instanceRunUuids.forEach(
          instanceRunUuid -> {
            WorkflowInstance instance =
                instanceDao.getWorkflowInstanceRun(
                    jobEvent.getWorkflowId(),
                    instanceRunUuid.getInstanceId(),
                    instanceRunUuid.getRunId());
            workflowRunner.run(instance, instanceRunUuid.getUuid());
          });
    }
    return Optional.empty();
  }

  @SuppressWarnings({"PMD.AvoidCatchingNPE"})
  private List<InstanceRunUuid> tryTerminateWorkflowInstances(TerminateThenRunJobEvent jobEvent) {
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
      TerminateThenRunJobEvent jobEvent,
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

  private void checkProgress(String workflowId, List<InstanceRunUuid> instanceRunUuids) {
    instanceRunUuids.forEach(
        runUuid -> {
          WorkflowInstance.Status status =
              instanceDao.getWorkflowInstanceStatus(
                  workflowId, runUuid.getInstanceId(), runUuid.getRunId());
          if (status == null || !status.isTerminal()) {
            throw new MaestroRetryableError(
                "Workflow instance [%s][%s] is still terminating and will check it again",
                workflowId, runUuid);
          }
        });
  }
}
