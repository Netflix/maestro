/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.engine.notebook;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.MapHelper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Build notebook params in JSON string format. This is used for input parameters for notebook
 * runtime.
 */
@Slf4j
@AllArgsConstructor
public class NotebookParamsBuilder {
  /** Excluded parameters. */
  private static final Set<String> EXCLUDED_PARAMS =
      Set.of(
          Constants.WORKFLOW_ID_PARAM,
          Constants.STEP_ATTEMPT_ID_PARAM,
          Constants.STEP_INSTANCE_UUID_PARAM,
          Constants.WORKFLOW_INSTANCE_ID_PARAM,
          Constants.STEP_SATISFIED_FIELD,
          Constants.FIRST_TIME_TRIGGER_TIMEZONE_PARAM,
          Constants.INITIATOR_TIMEZONE_PARAM);

  private final ObjectMapper objectMapper;
  private final List<NotebookParamsContributor> contributors;

  /**
   * Build notebook parameters and return the full JSON string.
   *
   * @return The full JSON string
   */
  public String buildNotebookParams(
      WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary, Step stepDefinition) {
    Map<String, Parameter> allParams = new LinkedHashMap<>();
    allParams.putAll(workflowSummary.getParams());
    allParams.putAll(runtimeSummary.getParams());

    Map<String, Object> paramMap = filterAndConvertToMap(allParams);
    if (stepDefinition != null && stepDefinition.getTimeout() != null) {
      paramMap.put(NotebookConstants.STEP_TIMEOUT, stepDefinition.getTimeout().asString());
    }
    if (workflowSummary.getCriticality() != null) {
      paramMap.put(
          NotebookConstants.CRITICALITY,
          workflowSummary.getCriticality().name().toLowerCase(Locale.US));
    }
    paramMap.put(NotebookConstants.ATTEMPT_NUMBER, runtimeSummary.getStepAttemptId() - 1);
    for (NotebookParamsContributor contributor : contributors) {
      contributor.contribute(workflowSummary, paramMap);
    }

    Map<String, Object> extraPapermillParams = generatePapermillParams(workflowSummary);
    paramMap.putAll(extraPapermillParams);

    return toPapermillParams(workflowSummary, paramMap);
  }

  /**
   * Generate extra Papermill parameters that should not be regular params, only go into the
   * notebook.
   *
   * @param workflowSummary the workflow summary
   * @return the generated papermill parameters
   */
  private Map<String, Object> generatePapermillParams(WorkflowSummary workflowSummary) {
    Map<String, Object> results = new LinkedHashMap<>();
    if (workflowSummary.getParams().containsKey(Constants.WORKFLOW_OWNER_PARAM)) {
      results.put(
          Constants.WORKFLOW_OWNER_PARAM,
          workflowSummary.getParams().get(Constants.WORKFLOW_OWNER_PARAM).asString());
    } else {
      results.put(
          Constants.WORKFLOW_OWNER_PARAM,
          Checks.notNull(workflowSummary.getRunProperties().getOwner(), "User cannot be null")
              .getName());
    }
    return results;
  }

  private String toPapermillParams(WorkflowSummary workflowSummary, Map<String, Object> input) {
    try {
      return objectMapper.writeValueAsString(input);
    } catch (JsonProcessingException e) {
      String workflowIdentity = workflowSummary.getIdentity();
      LOG.warn("Failed to serialize papermill params for workflow {}", workflowIdentity, e);
      throw new MaestroBadRequestException(
          e, "Failed to serialize papermill params for workflow %s", workflowIdentity);
    }
  }

  /**
   * Filter params that are neither in EXCLUDED_PARAMS nor empty string. Convert filtered params to
   * map.
   */
  private Map<String, Object> filterAndConvertToMap(Map<String, Parameter> params) {
    return params.entrySet().stream()
        .filter(p -> !EXCLUDED_PARAMS.contains(p.getKey()) && !isEmptyString(p.getValue()))
        .collect(MapHelper.toListMap(Map.Entry::getKey, e -> e.getValue().getEvaluatedResult()));
  }

  /** Check if the parameter is an empty string parameter. */
  private boolean isEmptyString(Parameter parameter) {
    return parameter.getType() == ParamType.STRING
        && (parameter.getEvaluatedResult() == null
            || parameter.getEvaluatedResultString().isEmpty());
  }
}
