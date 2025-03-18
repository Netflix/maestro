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
package com.netflix.maestro.engine.eval;

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.TimeInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.trigger.SignalTrigger;
import com.netflix.maestro.models.trigger.TimeTrigger;
import com.netflix.maestro.utils.Checks;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;

/** An internal wrapper class used to wrap the selected fields that can be returned by SEL calls. */
@Builder
public class InstanceWrapper {
  @Getter private final boolean isWorkflowParam;
  @Getter @NotNull private final String workflowId;
  @Getter private final Long workflowInstanceId;
  @Getter @NotNull private final Initiator initiator;
  @NotNull private final RunPolicy runPolicy;
  @NotNull private final RunProperties runProperties;
  private final Map<String, ParamDefinition> runParams;
  private final List<TimeTrigger> timeTriggers;
  private final List<SignalTrigger> signalTriggers;
  @Getter private final StepInstanceAttributes stepInstanceAttributes;

  /**
   * Build instance wrapper for the sel evaluation.
   *
   * @param workflowSummary workflow summary
   * @param stepRuntimeSummary step runtime summary
   * @return the instance wrapper object
   */
  public static InstanceWrapper from(
      WorkflowSummary workflowSummary, StepRuntimeSummary stepRuntimeSummary) {
    return InstanceWrapper.builder()
        .isWorkflowParam(false)
        .workflowId(workflowSummary.getWorkflowId())
        .workflowInstanceId(workflowSummary.getWorkflowInstanceId())
        .initiator(workflowSummary.getInitiator())
        .runPolicy(workflowSummary.getRunPolicy())
        .runProperties(workflowSummary.getRunProperties())
        .stepInstanceAttributes(StepInstanceAttributes.from(stepRuntimeSummary))
        .build();
  }

  /**
   * Build instance wrapper for the sel evaluation.
   *
   * @param instance workflow instance
   * @param request run workflow request
   * @return the instance wrapper object
   */
  public static InstanceWrapper from(WorkflowInstance instance, RunRequest request) {
    return InstanceWrapper.builder()
        .isWorkflowParam(true)
        .workflowId(instance.getWorkflowId())
        .initiator(request.getInitiator())
        .runPolicy(request.getCurrentPolicy())
        .runProperties(instance.getRunProperties())
        .runParams(request.getRunParams())
        .timeTriggers(instance.getRuntimeWorkflow().getTimeTriggers())
        .signalTriggers(instance.getRuntimeWorkflow().getSignalTriggers())
        .build();
  }

  String getInitiatorTimeZone() {
    if (initiator.getType() == Initiator.Type.TIME) {
      return ((TimeInitiator) initiator).getTimezone();
    }
    return null;
  }

  String getRunPolicy() {
    return runPolicy.name();
  }

  String getWorkflowOwner() {
    if (runParams != null && runParams.containsKey(Constants.WORKFLOW_OWNER_PARAM)) {
      // should be system initiated run
      if (initiator.getType().isRestartable()) {
        throw new MaestroBadRequestException(
            Collections.emptyList(),
            "Should not have OWNER set for non system initiated workflow id=[%s]",
            workflowId);
      }
      // owner info is a literal string
      ParamDefinition ownerParam = runParams.get(Constants.WORKFLOW_OWNER_PARAM);
      Checks.checkTrue(
          ownerParam.isLiteral(),
          "System initiator run should set owner as literal instead of using expression: [%s]",
          ownerParam.getExpression());
      return ownerParam.getValue();
    } else {
      return runProperties.getOwner().getName();
    }
  }

  String getFirstTimeTriggerTimeZone() {
    if (timeTriggers != null && !timeTriggers.isEmpty()) {
      return timeTriggers.getFirst().getTimezone();
    }
    return null;
  }

  void validateSignalName(String signalName) {
    boolean exist = false;
    if (signalTriggers != null && !signalTriggers.isEmpty()) {
      exist = signalTriggers.stream().anyMatch(s -> s.getDefinitions().containsKey(signalName));
    }
    Checks.checkTrue(
        exist,
        "Signal name [%s] does not exist in the signal trigger definition for workflow [%s]",
        signalName,
        workflowId);
  }

  void validateSignalParamName(String paramName) {
    boolean exist = false;
    if (signalTriggers != null && !signalTriggers.isEmpty()) {
      exist = signalTriggers.stream().anyMatch(s -> s.getParams().containsKey(paramName));
    }
    Checks.checkTrue(
        exist,
        "Signal param name [%s] does not exist in the signal trigger definition for workflow [%s]",
        paramName,
        workflowId);
  }
}
