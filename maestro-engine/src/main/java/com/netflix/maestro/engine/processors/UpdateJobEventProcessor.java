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
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.jobevents.NotificationJobEvent;
import com.netflix.maestro.queue.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.queue.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.queue.processors.MaestroEventProcessor;
import com.netflix.maestro.utils.IdHelper;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class UpdateJobEventProcessor implements MaestroEventProcessor<MaestroJobEvent> {
  private final TagPermitManager tagPermitManager;
  private final InstanceStepConcurrencyHandler instanceStepConcurrencyHandler;
  private final MaestroStepInstanceActionDao actionDao;
  private final FlowOperation flowOperation;
  private final MaestroQueueSystem queueSystem;

  @Override
  public Optional<MaestroJobEvent> process(MaestroJobEvent jobEvent) {
    switch (jobEvent.getType()) {
      case STEP_INSTANCE_UPDATE:
        processStepInstanceUpdateJobEvent((StepInstanceUpdateJobEvent) jobEvent);
        break;
      case WORKFLOW_INSTANCE_UPDATE:
        processWorkflowInstanceUpdateJobEvent((WorkflowInstanceUpdateJobEvent) jobEvent);
        break;
      case WORKFLOW_VERSION_UPDATE:
        processWorkflowVersionJobEvent((WorkflowVersionUpdateJobEvent) jobEvent);
        break;
      default:
        throw new MaestroInternalError(
            "Unsupported event type: [%s], ignoring this event: [%s]",
            jobEvent.getType(), jobEvent);
    }
    return Optional.of(NotificationJobEvent.create(jobEvent));
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
  }

  private void processWorkflowInstanceUpdateJobEvent(WorkflowInstanceUpdateJobEvent jobEvent) {
    boolean hasTerminal =
        jobEvent.getChangeRecords().stream()
            .map(
                changeEvent -> {
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
                      // if terminal non-root workflow, wake up the parent step at the best effort
                      wakeUpParentStep(changeEvent.getParent(), changeEvent.getGroupInfo());
                    } else { // root should remove all sets
                      instanceStepConcurrencyHandler.cleanUp(changeEvent.getCorrelationId());
                    }
                  }
                  return changeEvent.getNewStatus().isTerminal();
                })
            .reduce(false, Boolean::logicalOr);
    if (hasTerminal) {
      // once reaching a terminal state, emit a start workflow job event for non-foreach workflow
      enqueueStartWorkflowEvent(jobEvent.getWorkflowId());
    }
  }

  private void wakeUpParentStep(UpstreamInitiator.Info parent, long groupInfo) {
    if (parent == null) {
      return;
    }
    try {
      String flowReference = IdHelper.deriveFlowRef(parent.getWorkflowId(), parent.getInstanceId());
      long groupId = IdHelper.deriveGroupId(flowReference, groupInfo);
      flowOperation.wakeUp(groupId, flowReference, parent.getStepId());
    } catch (RuntimeException e) {
      LOG.info("Failed to wake up parent step {} and skip wakeup", parent, e);
    }
  }

  /**
   * Process workflow version update job event.
   *
   * @param jobEvent the job event
   */
  private void processWorkflowVersionJobEvent(WorkflowVersionUpdateJobEvent jobEvent) {
    enqueueStartWorkflowEvent(jobEvent.getWorkflowId());
  }

  /**
   * Helper method to publish a start workflow event for a given workflow if flag is true and
   * workflow id is valid and is not a foreach inline workflow.
   */
  private void enqueueStartWorkflowEvent(String workflowId) {
    if (workflowId != null && !workflowId.isEmpty() && !IdHelper.isInlineWorkflowId(workflowId)) {
      queueSystem.enqueueOrThrow(StartWorkflowJobEvent.create(workflowId));
    }
  }
}
