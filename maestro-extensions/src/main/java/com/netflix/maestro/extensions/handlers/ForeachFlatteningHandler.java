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
package com.netflix.maestro.extensions.handlers;

import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.extensions.dao.MaestroForeachFlattenedDao;
import com.netflix.maestro.extensions.dao.models.ForeachFlattenedInstance;
import com.netflix.maestro.extensions.dao.models.ForeachFlattenedModel;
import com.netflix.maestro.extensions.models.StepEventHandlerInput;
import com.netflix.maestro.extensions.provider.MaestroClient;
import com.netflix.maestro.extensions.utils.ForeachFlatteningHelper;
import com.netflix.maestro.extensions.utils.StepInstanceStatusEncoder;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class ForeachFlatteningHandler {
  private final MaestroForeachFlattenedDao maestroForeachFlattenedDao;
  private final MaestroClient maestroClient;
  private final ForeachFlatteningHelper foreachFlatteningHelper;

  public ForeachFlatteningHandler(
      MaestroClient maestroClient,
      MaestroForeachFlattenedDao maestroForeachFlattenedDao,
      ForeachFlatteningHelper foreachFlatteningHelper) {
    this.maestroClient = maestroClient;
    this.maestroForeachFlattenedDao = maestroForeachFlattenedDao;
    this.foreachFlatteningHelper = foreachFlatteningHelper;
  }

  public void process(StepEventHandlerInput stepEventHandlerInput) {
    WorkflowInstance workflowInstance = stepEventHandlerInput.workflowInstance();
    Initiator initiator = workflowInstance.getInitiator();
    try {
      if (initiator instanceof UpstreamInitiator && initiator.getType().isInline()) {
        String workflowId = ((UpstreamInitiator) initiator).getNonInlineParent().getWorkflowId();
        WorkflowDefinition workflowDefinition =
            maestroClient.getWorkflowDefinition(
                workflowId, String.valueOf(workflowInstance.getWorkflowVersionId()));
        StepInstance stepInstance =
            maestroClient.getWorkflowStepInstance(
                workflowInstance.getWorkflowId(),
                workflowInstance.getWorkflowInstanceId(),
                workflowInstance.getWorkflowRunId(),
                stepEventHandlerInput.stepId(),
                stepEventHandlerInput.stepAttemptId());
        process(stepEventHandlerInput, workflowDefinition, stepInstance);
      }
    } catch (MaestroNotFoundException e) {
      LOG.warn(
          "Didn't find workflowId={}, instanceId={}, runId={}, stepId={}, attemptId={} in maestro, either deleted or not created",
          workflowInstance.getWorkflowId(),
          workflowInstance.getWorkflowInstanceId(),
          workflowInstance.getWorkflowRunId(),
          stepEventHandlerInput.stepId(),
          stepEventHandlerInput.stepAttemptId());
    }
  }

  /**
   * Process the {@link StepEventHandlerInput} based on corresponding inline workflow instance, root
   * workflow definition and step instance.
   *
   * @param stepEventHandlerInput the {@link StepEventHandlerInput} to process
   * @param workflowDefinition the root {@link WorkflowDefinition} associated with this input.
   * @param stepInstance the {@link StepInstance} of this input.
   */
  public void process(
      StepEventHandlerInput stepEventHandlerInput,
      WorkflowDefinition workflowDefinition,
      StepInstance stepInstance) {
    WorkflowInstance inlineWorkflowInstance = stepEventHandlerInput.workflowInstance();
    UpstreamInitiator upstreamInitiator = (UpstreamInitiator) inlineWorkflowInstance.getInitiator();
    String workflowId = upstreamInitiator.getNonInlineParent().getWorkflowId();
    long instanceId = upstreamInitiator.getNonInlineParent().getInstanceId();
    long runId = upstreamInitiator.getNonInlineParent().getRunId();
    String stepId = stepEventHandlerInput.stepId();
    String iterationRank =
        foreachFlatteningHelper.getIterationRank(
            upstreamInitiator, inlineWorkflowInstance.getWorkflowInstanceId());
    Long createTime = stepInstance.getRuntimeState().getCreateTime();
    ForeachFlattenedInstance instance =
        new ForeachFlattenedInstance(
            workflowId, instanceId, runId, stepId, iterationRank, createTime);

    long runIdValidityEnd = Long.MAX_VALUE;
    long stepStatusEncoded =
        StepInstanceStatusEncoder.encode(stepInstance.getRuntimeState().getStatus());
    long statusPriority =
        StepInstanceStatusEncoder.getPriority(stepInstance.getRuntimeState().getStatus());
    String attemptSeq = foreachFlatteningHelper.getAttemptSeq(upstreamInitiator, stepInstance);
    Map<String, Object> loopParameters =
        foreachFlatteningHelper.getLoopParams(
            workflowDefinition, upstreamInitiator, inlineWorkflowInstance);
    ForeachFlattenedModel model =
        new ForeachFlattenedModel(
            instance,
            runIdValidityEnd,
            stepStatusEncoded,
            statusPriority,
            stepInstance.getRuntimeState(),
            attemptSeq,
            loopParameters);

    maestroForeachFlattenedDao.insertOrUpdateForeachFlattenedModel(model);
  }
}
