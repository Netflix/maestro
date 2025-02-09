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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroInvalidExpressionException;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.ParamHelper;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Parameter evaluator.
 *
 * <p>Params can reference each other in either String interpolation or SEL expression.
 *
 * <p>Maestro currently only support 2 scopes, workflow level scope and step level scope.
 *
 * <p>When Maestro does the parameter reference lookup, it always checks the current scope and then
 * if not found, it looks up one level up. For example, while evaluating a parameter in step level
 * scope, it firstly checks parameters of step level scope and if the parameter is not found by
 * name. It then checks parameters in workflow level. Additionally, it can reference parameters in
 * finished steps.
 */
@Slf4j
@AllArgsConstructor
public class ParamEvaluator {
  private static final String STEP_PARAM_SEPARATOR = "__";
  private static final String SIGNAL_EXPRESSION_TEMPLATE =
      "return params.getFromSignal('%s', '%s');";
  private static final String PARAM_NAME_FOR_ALL = "params";

  private final ExprEvaluator exprEvaluator;
  private final ObjectMapper objectMapper;

  /**
   * Evaluate workflow parameters.
   *
   * @param workflowParams workflow parameters to be evaluated
   * @param workflowId workflow id
   */
  public void evaluateWorkflowParameters(Map<String, Parameter> workflowParams, String workflowId) {
    for (Parameter param : workflowParams.values()) {
      parseWorkflowParameter(workflowParams, param, workflowId);
    }
    paramsSizeCheck(workflowParams, "workflow id: " + workflowId);
  }

  /**
   * Evaluate a given parameter using available workflow params.
   *
   * @param workflowId workflow id
   * @param workflowParams available workflow parameters
   * @param param parameter to be evaluated.
   */
  @VisibleForTesting
  void parseWorkflowParameter(
      Map<String, Parameter> workflowParams, Parameter param, String workflowId) {
    parseWorkflowParameter(workflowParams, param, workflowId, new HashSet<>());
  }

  private void parseWorkflowParameter(
      Map<String, Parameter> workflowParams,
      Parameter param,
      String workflowId,
      Set<String> visited) {
    if (!param.isEvaluated()) {
      Checks.checkTrue(
          visited.add(param.getName()),
          "In workflow [%s], param [%s] definition contains a cyclic reference chain",
          workflowId,
          param.getName());
      Object result = evaluateWorkflowParam(workflowParams, param, workflowId, visited);
      visited.remove(param.getName());
      setEvaluatedParam(param, result);
    }
  }

  private Object evaluateWorkflowParam(
      Map<String, Parameter> workflowParams,
      Parameter param,
      String workflowId,
      Set<String> visited) {
    if (!param.isLiteral()) {
      Set<String> refParamNames = exprEvaluator.validate(param.getExpression());
      refParamNames.remove(PARAM_NAME_FOR_ALL);
      Map<String, Parameter> refParams =
          getReferencedParams(param.getName(), refParamNames, workflowParams, workflowId, visited);
      Map<String, Object> usedParamValues =
          refParams.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getEvaluatedResult()));
      return exprEvaluator.eval(param.getExpression(), usedParamValues);
    } else if (param.getType() == ParamType.MAP) {
      Map<String, Object> result = new LinkedHashMap<>();
      for (Map.Entry<String, ParamDefinition> entry : param.asMapParam().getValue().entrySet()) {
        result.put(
            entry.getKey(),
            evaluateWorkflowParam(
                workflowParams, entry.getValue().toParameter(), workflowId, visited));
      }
      return result;
    } else {
      Set<String> refParamNames = LiteralEvaluator.getReferencedParamNames(param);
      Map<String, Parameter> refParams =
          getReferencedParams(param.getName(), refParamNames, workflowParams, workflowId, visited);
      return LiteralEvaluator.eval(param, refParams);
    }
  }

  private Map<String, Parameter> getReferencedParams(
      String paramName,
      Set<String> refParamNames,
      Map<String, Parameter> workflowParams,
      String workflowId,
      Set<String> visited) {
    Map<String, Parameter> refParams = new HashMap<>();
    for (String refParamName : refParamNames) {
      if (workflowParams.containsKey(refParamName)) {
        // support recursive call but also keep workflow parameter definition ordering
        parseWorkflowParameter(
            workflowParams, workflowParams.get(refParamName), workflowId, visited);
        refParams.put(refParamName, workflowParams.get(refParamName));
      } else if (refParamName.contains(STEP_PARAM_SEPARATOR)) {
        // here it might be from signal triggers
        Map.Entry<String, String> pair = parseReferenceName(refParamName, Collections.emptyMap());
        refParams.put(
            refParamName, getReferenceSignalParam(refParamName, pair.getKey(), pair.getValue()));
      } else {
        throw new MaestroValidationException(
            "Param [%s] referenced a non-existing param [%s] in workflow [%s]",
            paramName, refParamName, workflowId);
      }
    }
    return refParams;
  }

  private void setEvaluatedParam(Parameter param, Object result) {
    param.setEvaluatedResult(result);
    param.setEvaluatedTime(System.currentTimeMillis());
  }

  /**
   * Evaluate step parameters, including sel or string interpolation.
   *
   * @param allStepOutputData parameter definitions from all step definitions in the workflow
   * @param workflowParams all evaluated workflow instance params
   * @param stepParams all step runtime params to be evaluated
   */
  public void evaluateStepParameters(
      Map<String, Map<String, Object>> allStepOutputData,
      Map<String, Parameter> workflowParams,
      Map<String, Parameter> stepParams,
      String stepId) {
    for (Parameter param : stepParams.values()) {
      parseStepParameter(allStepOutputData, workflowParams, stepParams, param, stepId);
    }
    paramsSizeCheck(stepParams, "step id: " + stepId);
  }

  /**
   * Evaluate input parameters, including sel or string interpolation.
   *
   * @param allStepOutputData parameter definitions from all step definitions in the workflow
   * @param workflowParams all evaluated workflow instance params
   * @param stepParams all evaluated step params
   * @param parameters all step dependencies params to be evaluated
   */
  public void evaluateStepDependenciesOrOutputsParameters(
      Map<String, Map<String, Object>> allStepOutputData,
      Map<String, Parameter> workflowParams,
      Map<String, Parameter> stepParams,
      Collection<List<MapParameter>> parameters,
      String stepId) {
    parameters.stream()
        .flatMap(Collection::stream)
        .forEach(v -> parseStepParameter(allStepOutputData, workflowParams, stepParams, v, stepId));
  }

  /**
   * Evaluate a step parameter, including sel or string interpolation.
   *
   * @param allStepOutputData parameter definitions from all step definitions in the workflow
   * @param workflowParams all evaluated workflow instance params
   * @param stepParams all step runtime params
   * @param param a param tto be evaluated
   */
  public void parseStepParameter(
      Map<String, Map<String, Object>> allStepOutputData,
      Map<String, Parameter> workflowParams,
      Map<String, Parameter> stepParams,
      Parameter param,
      String stepId) {
    parseStepParameter(
        allStepOutputData, workflowParams, stepParams, param, stepId, new HashSet<>());
  }

  /**
   * Evaluate a step parameter, including sel or string interpolation with cyclic reference
   * detection.
   */
  private void parseStepParameter(
      Map<String, Map<String, Object>> allStepOutputData,
      Map<String, Parameter> workflowParams,
      Map<String, Parameter> stepParams,
      Parameter param,
      String stepId,
      Set<String> visited) {
    if (!param.isEvaluated()) {
      Checks.checkTrue(
          visited.add(param.getName()),
          "In step [%s], param [%s] definition contains a cyclic reference chain",
          stepId,
          param.getName());
      Object result =
          evaluateStepParam(allStepOutputData, workflowParams, stepParams, param, stepId, visited);
      visited.remove(param.getName());
      setEvaluatedParam(param, result);
    }
  }

  private Object evaluateStepParam(
      Map<String, Map<String, Object>> allStepOutputData,
      Map<String, Parameter> workflowParams,
      Map<String, Parameter> stepParams,
      Parameter param,
      String stepId,
      Set<String> visited) {
    if (!param.isLiteral()) {
      Set<String> refParamNames = exprEvaluator.validate(param.getExpression());
      Map<String, Parameter> refParams =
          getReferencedParams(
              refParamNames,
              allStepOutputData,
              workflowParams,
              stepParams,
              param.getName(),
              stepId,
              visited);
      Map<String, Object> usedParamValues =
          refParams.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getEvaluatedResult()));
      return exprEvaluator.eval(param.getExpression(), usedParamValues);
    } else if (param.getType() == ParamType.MAP) {
      Map<String, Object> result = new LinkedHashMap<>();
      for (Map.Entry<String, ParamDefinition> entry : param.asMapParam().getValue().entrySet()) {
        result.put(
            entry.getKey(),
            evaluateStepParam(
                allStepOutputData,
                workflowParams,
                stepParams,
                entry.getValue().toParameter().unwrap(),
                stepId,
                visited));
      }
      return result;
    } else {
      // support string interpolation, also support `__`
      Set<String> refParamNames = LiteralEvaluator.getReferencedParamNames(param);
      Map<String, Parameter> refParams =
          getReferencedParams(
              refParamNames,
              allStepOutputData,
              workflowParams,
              stepParams,
              param.getName(),
              stepId,
              visited);
      return LiteralEvaluator.eval(param, refParams);
    }
  }

  private Map<String, Parameter> getReferencedParams(
      Set<String> refParamNames,
      Map<String, Map<String, Object>> allStepOutputData,
      Map<String, Parameter> workflowParams,
      Map<String, Parameter> stepParams,
      String paramName,
      String stepId,
      Set<String> visited) {
    Map<String, Parameter> usedParams = new HashMap<>();
    for (String refParam : refParamNames) {
      if (refParam.contains(STEP_PARAM_SEPARATOR)) {
        usedParams.put(
            refParam,
            getReferenceParam(
                allStepOutputData,
                workflowParams,
                stepParams,
                paramName,
                refParam,
                stepId,
                visited));
      } else if (PARAM_NAME_FOR_ALL.equals(refParam)) {
        usedParams.putAll(workflowParams);
      } else if (stepParams.containsKey(refParam) && stepParams.get(refParam).isEvaluated()) {
        usedParams.put(refParam, stepParams.get(refParam));
      } else if (workflowParams.containsKey(refParam)) {
        // workflow params have been evaluated before any step
        usedParams.put(refParam, workflowParams.get(refParam));
      } else if (stepParams.containsKey(refParam)) {
        // support recursive call but also keep workflow parameter definition ordering
        parseStepParameter(
            allStepOutputData,
            workflowParams,
            stepParams,
            stepParams.get(refParam),
            stepId,
            visited);
        usedParams.put(refParam, stepParams.get(refParam));
      } else {
        throw new MaestroInternalError(
            "Cannot find the referenced param [%s] in step [%s] or upstream steps or the workflow",
            refParam, stepId);
      }
    }
    return usedParams;
  }

  /** Extract step id and param name from the reference. It handles `__`, `___`, and `____`. */
  private Map.Entry<String, String> parseReferenceName(
      String refParam, Map<String, Map<String, Object>> allStepOutputData) {
    int idx1 = refParam.indexOf(STEP_PARAM_SEPARATOR);
    int idx2 = refParam.lastIndexOf(STEP_PARAM_SEPARATOR);
    //  ___ or ____ case
    if (idx1 != idx2) {
      String id1 = refParam.substring(0, idx1);
      String id2 = refParam.substring(0, idx1 + 1);
      boolean flag1 = allStepOutputData.containsKey(id1);
      boolean flag2 = allStepOutputData.containsKey(id2);
      if (!flag1 && flag2) {
        idx1 += 1;
      } else if (flag1 && flag2) {
        throw new MaestroInternalError(
            "reference [%s] cannot be parsed due to ambiguity (both step ids [%s] and [%s] exist in the workflow",
            refParam, id1, id2);
      } else if (!flag1) {
        throw new MaestroInternalError(
            "reference [%s] cannot be parsed as cannot find either step id [%s] or [%s] in the workflow",
            refParam, id1, id2);
      }
    }
    String refStepId = refParam.substring(0, idx1);
    String refParamName = refParam.substring(idx1 + 2);
    return new AbstractMap.SimpleEntry<>(refStepId, refParamName);
  }

  private Parameter getReferenceParam(
      Map<String, Map<String, Object>> allStepOutputData,
      Map<String, Parameter> workflowParams,
      Map<String, Parameter> stepParams,
      String paramName,
      String refParam,
      String stepId,
      Set<String> visited) {
    Map.Entry<String, String> pair = parseReferenceName(refParam, allStepOutputData);
    String refStepId = pair.getKey();
    String refParamName = pair.getValue();
    if (Objects.equals(refStepId, stepId)) {
      if (stepParams.containsKey(refParamName)) {
        parseStepParameter(
            allStepOutputData,
            workflowParams,
            stepParams,
            stepParams.get(refParamName),
            stepId,
            visited);
        return stepParams.get(refParamName);
      } else {
        throw new MaestroInternalError(
            "Param [%s] referenced a non-existing param [%s] in step [%s]",
            paramName, refParam, stepId);
      }
    }

    // here it might be from signal triggers or signal dependencies (not supported yet)
    if (!allStepOutputData.containsKey(refStepId)) {
      return getReferenceSignalParam(refParam, refStepId, refParamName);
    }

    Map<String, Object> refStepData =
        Checks.notNull(
            allStepOutputData.get(refStepId),
            "Error: param [%s] in step [%s] referenced a non-existing step [%s] in the expression [%s]",
            paramName,
            stepId,
            refStepId,
            refParam);
    StepRuntimeSummary refRuntimeSummary =
        StepHelper.retrieveRuntimeSummary(objectMapper, refStepData);
    Parameter refStepParam =
        Checks.notNull(
            refRuntimeSummary.getParams().get(refParamName),
            "Cannot find the referenced param name [%s] in step [%s]",
            refParamName,
            refStepId);

    Checks.checkTrue(
        refStepParam.isEvaluated(),
        "Referenced param [%s] in step [%s] is not evaluated yet. "
            + "Note that a step can only reference its upstream step's param.",
        refParamName,
        refStepId);

    return refStepParam;
  }

  /**
   * Extract signal param value from the signal and wrap it into a Parameter. Currently, it only
   * returns a StringParameter.
   */
  @SuppressWarnings({"PMD.PreserveStackTrace"})
  private Parameter getReferenceSignalParam(String refParam, String signalName, String paramName) {
    try {
      String expr = String.format(SIGNAL_EXPRESSION_TEMPLATE, signalName, paramName);
      Object val = exprEvaluator.eval(expr, Collections.emptyMap());
      Parameter param =
          ParamHelper.deriveTypedParameter(
              paramName, expr, val, null, ParamMode.IMMUTABLE, Collections.emptyMap());
      param.setEvaluatedResult(val);
      param.setEvaluatedTime(System.currentTimeMillis());
      return param;
    } catch (MaestroRuntimeException e) {
      LOG.warn(
          "Failed to evaluate [{}] as a param within signal triggers or dependencies due to",
          refParam,
          e);
      throw new MaestroInvalidExpressionException(
          "Failed to evaluate the param with a definition: [%s]. Please check if there is a typo in the expression.",
          refParam);
    }
  }

  /**
   * Run string interpolation on input using the evaluated params for attribute fields, e.g. tct,
   * timeout, etc.
   */
  public Parameter parseAttribute(
      ParamDefinition paramDef,
      Map<String, Parameter> evaluatedParams,
      String workflowId,
      boolean ignoreError) {
    try {
      Parameter param = paramDef.toParameter();
      parseWorkflowParameter(evaluatedParams, param, workflowId);
      return param;
    } catch (MaestroRuntimeException error) {
      if (ignoreError) {
        LOG.warn(
            "Ignore the error while parsing attribute [{}] for workflow [{}] due to ",
            paramDef.getName(),
            workflowId,
            error);
        return null;
      }
      throw new MaestroUnprocessableEntityException(
          "Failed to parse attribute [%s] for workflow [%s] due to %s",
          paramDef.getName(), workflowId, error);
    }
  }

  private void paramsSizeCheck(Map<String, Parameter> params, String id) {
    try {
      long size = objectMapper.writeValueAsString(params).length();
      Checks.checkTrue(
          size <= Constants.JSONIFIED_PARAMS_STRING_SIZE_LIMIT,
          "Parameters' total size [%s] is larger than system limit [%s]",
          size,
          Constants.JSONIFIED_PARAMS_STRING_SIZE_LIMIT);
    } catch (JsonProcessingException e) {
      throw new MaestroInternalError(e, "cannot parse params into JSON for " + id);
    }
  }
}
