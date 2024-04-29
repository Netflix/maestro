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

import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.concurrency.TagPermitManager;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import com.netflix.maestro.engine.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.events.MaestroEvent;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PublishJobEventProcessor implements MaestroEventProcessor<MaestroJobEvent> {

  private final WorkflowHelper workflowHelper;
  private final MaestroNotificationPublisher eventClient;
  private final TagPermitManager tagPermitManager;
  private final MaestroStepInstanceActionDao actionDao;

  private final InstanceStepConcurrencyHandler instanceStepConcurrencyHandler;

  private final String clusterName;

  @Override
  public void process(Supplier<MaestroJobEvent> messageSupplier) {
    MaestroJobEvent jobEvent = messageSupplier.get();
    switch (jobEvent.getType()) {
      case STEP_INSTANCE_UPDATE_JOB_EVENT:
        processStepInstanceUpdateJobEvent((StepInstanceUpdateJobEvent) jobEvent);
        break;
      case WORKFLOW_INSTANCE_UPDATE_JOB_EVENT:
        processWorkflowInstanceUpdateJobEvent((WorkflowInstanceUpdateJobEvent) jobEvent);
        break;
      case WORKFLOW_VERSION_UPDATE_JOB_EVENT:
        processWorkflowVersionJobEvent((WorkflowVersionUpdateJobEvent) jobEvent);
        break;
      default:
        throw new MaestroInternalError(
            "Unsupported event type: %s , ignoring this event: %s", jobEvent.getType(), jobEvent);
    }
  }

  private void processStepInstanceUpdateJobEvent(StepInstanceUpdateJobEvent jobEvent) {
    if (jobEvent.hasTerminal()) {
      // Once reaching a terminal state, release tag permits held by this step.
      tagPermitManager.releaseTagPermits(jobEvent.getStepUuid());
      actionDao.cleanUp(jobEvent); // clean up its step action
      // unregister the step from instance_step_concurrency step uuid set
      instanceStepConcurrencyHandler.unregisterStep(
          jobEvent.getCorrelationId(), jobEvent.getStepUuid());
    }

    StepInstanceStatusChangeEvent changeEvent = jobEvent.toMaestroEvent(clusterName);
    eventClient.send(changeEvent);
  }

  private void processWorkflowInstanceUpdateJobEvent(WorkflowInstanceUpdateJobEvent jobEvent) {
    boolean hasTerminal =
        jobEvent
            .toMaestroEventStream(clusterName)
            .map(
                changeEvent -> {
                  eventClient.send(changeEvent);
                  if (changeEvent.getNewStatus().isTerminal()) {
                    // clean up any step actions for a workflow instance run in a terminal state
                    actionDao.cleanUp(
                        jobEvent.getWorkflowId(),
                        changeEvent.getWorkflowInstanceId(),
                        changeEvent.getWorkflowRunId());
                    // remove it from the instance_step_concurrency instance uuid set
                    if (changeEvent.getDepth() > 0) {
                      instanceStepConcurrencyHandler.removeInstance(
                          changeEvent.getCorrelationId(),
                          changeEvent.getDepth(),
                          changeEvent.getWorkflowUuid());
                    } else { // root should remove all sets
                      instanceStepConcurrencyHandler.cleanUp(changeEvent.getCorrelationId());
                    }
                  }
                  return changeEvent.getNewStatus().isTerminal();
                })
            .reduce(false, Boolean::logicalOr);
    if (hasTerminal) {
      // once reaching a terminal state, emit a start workflow job event for non-foreach workflow
      workflowHelper.publishStartWorkflowEvent(jobEvent.getWorkflowId(), true);
    }
  }

  private void processWorkflowVersionJobEvent(WorkflowVersionUpdateJobEvent jobEvent) {
    workflowHelper.publishStartWorkflowEvent(jobEvent.getWorkflowId(), true);

    MaestroEvent changeEvent = jobEvent.toMaestroEvent(clusterName);
    eventClient.send(changeEvent);
  }
}
