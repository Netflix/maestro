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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.extensions.ExtensionsBaseTest;
import com.netflix.maestro.extensions.dao.MaestroForeachFlattenedDao;
import com.netflix.maestro.extensions.dao.models.ForeachFlattenedInstance;
import com.netflix.maestro.extensions.dao.models.ForeachFlattenedModel;
import com.netflix.maestro.extensions.models.StepEventHandlerInput;
import com.netflix.maestro.extensions.provider.MaestroClient;
import com.netflix.maestro.extensions.utils.ForeachFlatteningHelper;
import com.netflix.maestro.extensions.utils.StepInstanceStatusEncoder;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator.Info;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepInstance.Status;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ForeachFlatteningHandlerTest extends ExtensionsBaseTest {
  @Mock private WorkflowInstance inlineWorkflowInstance;
  @Mock private WorkflowDefinition workflowDefinition;
  @Mock private StepInstance stepInstance;
  @Mock private MaestroClient maestroClient;
  @Mock private MaestroForeachFlattenedDao foreachFlattenedDao;
  @Mock private ForeachFlatteningHelper foreachFlatteningHelper;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testShouldPopulateFieldsForStepInstanceUpdateEvent() {
    String workflowId = "maestro_foreach_inline_id";
    long workflowInstanceId = 2;
    long workflowRunId = 3;
    String leafStepId = "step-id";
    long stepAttemptId = 5;
    Map<String, Object> loopParams = new HashMap<>();
    loopParams.put("num", 22);
    loopParams.put("inner_num", 33);

    UpstreamInitiator initiator = buildForeachInitiator();
    var rootWorkflowInfo = initiator.getNonInlineParent();
    var rootWorkflowId = rootWorkflowInfo.getWorkflowId();
    var rootWorkflowInstanceId = rootWorkflowInfo.getInstanceId();
    var rootWorkflowRunId = rootWorkflowInfo.getRunId();
    var nestedInstanceId = initiator.getAncestors().getLast().getInstanceId();
    StepRuntimeState runtimeState = new StepRuntimeState();
    long createTime = 1700717585499L;
    runtimeState.setCreateTime(createTime);
    Status stepStatus = Status.CREATED;
    runtimeState.setStatus(stepStatus);
    String iterationRank = String.format("1%d-1%d", nestedInstanceId, workflowInstanceId);
    String attemptSeq = "11-12-D33456000999898-223-211-15";

    when(inlineWorkflowInstance.getWorkflowId()).thenReturn(workflowId);
    when(inlineWorkflowInstance.getWorkflowInstanceId()).thenReturn(workflowInstanceId);
    when(inlineWorkflowInstance.getWorkflowRunId()).thenReturn(workflowRunId);
    when(inlineWorkflowInstance.getInitiator()).thenReturn(initiator);
    long workflowVersionId = 21L;
    when(inlineWorkflowInstance.getWorkflowVersionId()).thenReturn(workflowVersionId);
    when(maestroClient.getWorkflowStepInstance(
            workflowId, workflowInstanceId, workflowRunId, leafStepId, stepAttemptId))
        .thenReturn(stepInstance);
    when(stepInstance.getRuntimeState()).thenReturn(runtimeState);
    long immediateInlineWorkflowRunId = 8L;
    when(stepInstance.getWorkflowRunId()).thenReturn(immediateInlineWorkflowRunId);
    long leafStepAttemptId = 1L;
    when(stepInstance.getStepAttemptId()).thenReturn(leafStepAttemptId);
    when(maestroClient.getWorkflowDefinition(rootWorkflowId, String.valueOf(workflowVersionId)))
        .thenReturn(workflowDefinition);
    when(foreachFlatteningHelper.getLoopParams(
            workflowDefinition, initiator, inlineWorkflowInstance))
        .thenReturn(loopParams);
    when(foreachFlatteningHelper.getIterationRank(initiator, workflowInstanceId))
        .thenReturn(iterationRank);
    when(foreachFlatteningHelper.getAttemptSeq(initiator, stepInstance)).thenReturn(attemptSeq);

    // Calling service to insert event in DAO after processing
    ForeachFlatteningHandler service =
        new ForeachFlatteningHandler(maestroClient, foreachFlattenedDao, foreachFlatteningHelper);
    service.process(new StepEventHandlerInput(inlineWorkflowInstance, leafStepId, stepAttemptId));

    // Asserting ForeachFlattenedModel used to insert in DAO
    ForeachFlattenedInstance expectedInstance =
        new ForeachFlattenedInstance(
            rootWorkflowId,
            rootWorkflowInstanceId,
            rootWorkflowRunId,
            leafStepId,
            iterationRank,
            createTime);
    long expectedRunIdValidityEnd = Long.MAX_VALUE;
    ForeachFlattenedModel expectedModel =
        new ForeachFlattenedModel(
            expectedInstance,
            expectedRunIdValidityEnd,
            StepInstanceStatusEncoder.encode(stepStatus),
            StepInstanceStatusEncoder.getPriority(stepStatus),
            runtimeState,
            attemptSeq,
            loopParams);
    verify(foreachFlattenedDao).insertOrUpdateForeachFlattenedModel(expectedModel);
  }

  @Test
  public void testWorkflowNotFound() {
    String leafStepId = "step-id";
    long stepAttemptId = 5;
    UpstreamInitiator initiator = buildForeachInitiator();
    when(inlineWorkflowInstance.getInitiator()).thenReturn(initiator);
    long workflowVersionId = 21L;
    when(inlineWorkflowInstance.getWorkflowVersionId()).thenReturn(workflowVersionId);
    when(maestroClient.getWorkflowDefinition(anyString(), anyString()))
        .thenThrow(new MaestroNotFoundException("workflow not found"));
    ForeachFlatteningHandler service =
        new ForeachFlatteningHandler(maestroClient, foreachFlattenedDao, foreachFlatteningHelper);
    service.process(new StepEventHandlerInput(inlineWorkflowInstance, leafStepId, stepAttemptId));
    verify(foreachFlattenedDao, never()).insertOrUpdateForeachFlattenedModel(any());
  }

  private ForeachInitiator buildForeachInitiator() {
    var initiator = new ForeachInitiator();
    Info rootInfo = new Info();
    String rootWorkflowId = "root-workflow-id";
    rootInfo.setWorkflowId(rootWorkflowId);
    String rootStepId = "root_step-id";
    rootInfo.setStepId(rootStepId);
    int rootWorkflowInstanceId = 10;
    rootInfo.setInstanceId(rootWorkflowInstanceId);
    int rootWorkflowRunId = 2;
    rootInfo.setRunId(rootWorkflowRunId);
    int rootForeachStepAttemptId = 1;
    rootInfo.setStepAttemptId(rootForeachStepAttemptId);

    Info nestedInfo = new Info();
    nestedInfo.setWorkflowId("maestro_foreach_nested_inline_id");
    nestedInfo.setStepId("nested-step-id");
    int nestedInstanceId = 5;
    nestedInfo.setInstanceId(nestedInstanceId);
    int nestedWorkflowRunId = 3;
    nestedInfo.setRunId(nestedWorkflowRunId);
    int nestedForeachStepAttemptId = 2;
    nestedInfo.setStepAttemptId(nestedForeachStepAttemptId);

    List<Info> ancestors = new ArrayList<>();
    ancestors.add(rootInfo);
    ancestors.add(nestedInfo);
    initiator.setAncestors(ancestors);
    return initiator;
  }
}
