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
package com.netflix.maestro.extensions.utils;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.ForeachStep;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator.Info;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;

public class ForeachFlatteningHelper {
  private static final String ITERATION_ID_DELIMITER = "-";

  /**
   * Maximum number of digits for iteration we can encode in the iteration rank. So we can encode
   * foreach iteration ids until (1e71 - 1) using current encoding scheme.
   */
  private static final int MAX_ITERATION_ID_ENCODE_LENGTH = 71;

  /**
   * This is the limit on iteration id digits length we use to encode using digits from 1 to 9,
   * after this we use A - Z and further ASCII characters for encoding length of digits.
   */
  private static final int LENGTH_LIMIT_FOR_NUMBER_BASED_ENCODING = 10;

  /**
   * Get the loop params names and values for the leaf inline workflow instance, root non-inline
   * workflow definition and the initiator information.
   *
   * @param definition the root workflow definition
   * @param upstreamInitiator the upstream initiator
   * @param workflowInstance the leaf inline workflow instance
   * @return a map of loop param names and values
   */
  public Map<String, Object> getLoopParams(
      @NonNull WorkflowDefinition definition,
      @NonNull UpstreamInitiator upstreamInitiator,
      @NonNull WorkflowInstance workflowInstance) {
    List<Info> ancestors = upstreamInitiator.getAncestors();
    String rootWorkflowId = upstreamInitiator.getNonInlineParent().getWorkflowId();
    Set<String> upstreamSteps = new HashSet<>();
    for (int i = ancestors.size() - 1; i >= 0; --i) {
      upstreamSteps.add(ancestors.get(i).getStepId());
      if (ancestors.get(i).getWorkflowId().equals(rootWorkflowId)) {
        break;
      }
    }
    List<Step> steps = definition.getWorkflow().getSteps();
    List<String> loopParamNames = new ArrayList<>();
    getLoopParamNames(steps, upstreamSteps, loopParamNames);
    Map<String, Parameter> paramsMap = workflowInstance.getParams();
    Map<String, Object> result = new HashMap<>();
    for (String name : loopParamNames) {
      if (paramsMap.containsKey(name)) {
        Parameter parameter = paramsMap.get(name);
        result.put(name, parameter.getValue());
      }
    }
    return result;
  }

  /**
   * Gets the attemptSequence indicating the sequence of runId-stepAttemptId... from the root
   * non-inline workflow to the immediate inline workflow. For example, if the first root non-inline
   * workflow run id is 11 and the root foreach step attempt id is 2, and the first level inline
   * workflow run id is 3 and the first level foreach step attempt id is 1, and the leaf (final
   * level) inline workflow run id is 1 and the leaf (final level) step attempt id is 9 then the
   * attempt sequence will be 211-12-13-11-11-19. The format is of
   * [rootRunId]-[rootStepAttemptId]-[nestedRunId1]-[nestedStepAttemptId1]-...-[leafRunId]-[leafStepAttemptId].
   * Each of these id in [id] is encoded using length encoding, the length of digits is prepended to
   * the encoded id. So the 211 is the encoding for rootRunId 11, as the length of digits is 2.
   *
   * @param upstreamInitiator the upstream initiator
   * @param stepInstance the leaf step instance in a foreach nested step
   * @return the encoded attemptSeq
   */
  public String getAttemptSeq(
      @NonNull UpstreamInitiator upstreamInitiator, @NonNull StepInstance stepInstance) {
    List<String> encoded = new ArrayList<>();
    // encode current workflow run id and step attempt id.
    encoded.add(
        encodeRunIdAndAttemptId(stepInstance.getWorkflowRunId(), stepInstance.getStepAttemptId()));
    String rootWorkflowId = upstreamInitiator.getNonInlineParent().getWorkflowId();
    List<UpstreamInitiator.Info> ancestors = upstreamInitiator.getAncestors();
    for (int i = ancestors.size() - 1; i >= 0; --i) {
      long runIdStr = ancestors.get(i).getRunId();
      long attemptStr = ancestors.get(i).getStepAttemptId();
      encoded.add(encodeRunIdAndAttemptId(runIdStr, attemptStr));
      if (ancestors.get(i).getWorkflowId().equals(rootWorkflowId)) {
        break;
      }
    }
    Collections.reverse(encoded);
    return String.join(ITERATION_ID_DELIMITER, encoded);
  }

  /**
   * Gets the iteration rank indicating the sequence of iteration1-iteration2... from the first
   * inline workflow to the last inline workflow. For example, if the first inline workflow instance
   * id (iteration id) is 344 and the nested inline workflow iteration id is 3 and the final nested
   * inline workflow iteration id is 23 then the iteration rank will be 3344-13-23. The format is of
   * [iterationId1]-[iterationId2]-...-[leafIterationId]. Each of these id in [id] is encoded using
   * length encoding, the length of digits is prepended to the encoded id. So the 3344 is the
   * encoding for iteration id 344, as the length of digits is 3.
   *
   * @param upstreamInitiator the upstream initiator
   * @param instanceId the leaf inline workflow instance id
   * @return the encoded iteration rank
   */
  public String getIterationRank(@NonNull UpstreamInitiator upstreamInitiator, long instanceId) {
    String rootWorkflowId = upstreamInitiator.getNonInlineParent().getWorkflowId();
    List<Info> ancestors = upstreamInitiator.getAncestors();
    List<String> iterationRank = new ArrayList<>();
    iterationRank.add(encodeByLength(String.valueOf(instanceId)));
    for (int i = ancestors.size() - 1; i >= 0; --i) {
      if (ancestors.get(i).getWorkflowId().equals(rootWorkflowId)) {
        break;
      }
      iterationRank.add(encodeByLength(Long.toString(ancestors.get(i).getInstanceId())));
    }
    Collections.reverse(iterationRank);
    return String.join(ITERATION_ID_DELIMITER, iterationRank);
  }

  public static String getIterationRank(String iterationId) {
    if (iterationId == null || iterationId.isEmpty()) {
      return null;
    }
    return Stream.of(iterationId.split(ITERATION_ID_DELIMITER))
        .map(ForeachFlatteningHelper::encodeByLength)
        .collect(Collectors.joining(ITERATION_ID_DELIMITER));
  }

  private void getLoopParamNames(
      List<Step> steps, Set<String> upstreamSteps, List<String> loopParamNames) {
    for (Step step : steps) {
      if (step.getType() == StepType.FOREACH && upstreamSteps.contains(step.getId())) {
        Set<String> paramNames =
            step.getParams().get(Constants.LOOP_PARAMS_NAME).asMapParamDef().getValue().keySet();
        for (String name : paramNames) {
          if (!loopParamNames.contains(name)) {
            loopParamNames.add(name);
          }
        }
        upstreamSteps.remove(step.getId());
        getLoopParamNames(((ForeachStep) step).getSteps(), upstreamSteps, loopParamNames);
      }
    }
  }

  private String encodeRunIdAndAttemptId(long runId, long attemptId) {
    return encodeByLength(String.valueOf(runId))
        + ITERATION_ID_DELIMITER
        + encodeByLength(String.valueOf(attemptId));
  }

  private static String encodeByLength(String s) {
    if (s.length() > MAX_ITERATION_ID_ENCODE_LENGTH) {
      throw new IllegalArgumentException("The input number exceeds max length: " + s.length());
    }
    char len =
        s.length() < LENGTH_LIMIT_FOR_NUMBER_BASED_ENCODING
            ? (char) ('0' + s.length())
            : (char) ('A' + (s.length() - LENGTH_LIMIT_FOR_NUMBER_BASED_ENCODING));
    return len + s;
  }
}
