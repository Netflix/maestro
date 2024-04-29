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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.initiator.SignalInitiator;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.utils.Checks;
import com.netflix.sel.ext.AbstractParamExtension;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;

/**
 * Maestro param extension wrapper. To be thread safe, create a new one for every evaluation. This
 * class should be read-only and not mutate any states of the input. Also, it takes its own security
 * risks as it is not restricted by SEL security feature.
 */
@SuppressWarnings({"PMD.DoNotUseThreads", "PMD.BeanMembersShouldSerialize"})
@AllArgsConstructor
public class MaestroParamExtension extends AbstractParamExtension {
  private static final int RANDOM_JITTER_DELAY = 10;
  private static final int TIMEOUT_IN_MILLIS = 90000;
  private static final String GET_INSTANCE_FIELD_ERROR_MESSAGE =
      "getFromInstance('%s') call can only be used to define a workflow parameter";
  static final Long DUMMY_VALIDATION_VALUE = 20220101L;

  /** Function to return the next unique id. */
  static final String NEXT_UNIQUE_ID = "nextUniqueId";

  /** Function to return the execution environment. */
  static final String GET_EXEC_ENVIRONMENT = "getExecutionEnvironment";

  /** Function to get the instance metadata like workflow_id. */
  static final String GET_FROM_INSTANCE = "getFromInstance";

  /** Function to get the step metadata like step_id, step_uuid. */
  static final String GET_FROM_STEP = "getFromStep";

  /** Function to get the signal metadata like signal_param. */
  static final String GET_FROM_SIGNAL = "getFromSignal";

  /** Function to get the signal dependency metadata. */
  static final String GET_FROM_SIGNAL_DEPENDENCY = "getFromSignalDependency";

  /** Function to get the signal metadata for return default value if not found. */
  static final String GET_FROM_SIGNAL_OR_DEFAULT = "getFromSignalOrDefault";

  /** Function to get the foreach metadata. */
  static final String GET_FROM_FOREACH = "getFromForeach";

  private final ExecutorService executor;
  private final MaestroStepInstanceDao stepInstanceDao;
  private final String env;
  private final Map<String, Map<String, Object>> allStepOutputData;
  private final Map<String, List<Map<String, Parameter>>> signalDependenciesParams;
  private final InstanceWrapper instanceWrapper;
  private final ObjectMapper objectMapper;

  @Override
  protected Object callWithoutArg(String methodName) {
    if (NEXT_UNIQUE_ID.equals(methodName)) {
      return nextUniqueId();
    } else if (GET_EXEC_ENVIRONMENT.equals(methodName)) {
      return env;
    }
    throw new UnsupportedOperationException(
        "MaestroParamExtension DO NOT support calling method: " + methodName);
  }

  @Override
  protected Object callWithOneArg(String methodName, String arg1) {
    if (GET_FROM_INSTANCE.equals(methodName)) {
      return getFromInstance(arg1);
    } else if (GET_FROM_STEP.equals(methodName)) {
      return getFromStep(arg1);
    }
    throw new UnsupportedOperationException(
        String.format(
            "MaestroParamExtension DO NOT support calling method: %s with args: %s",
            methodName, arg1));
  }

  @Override
  protected Object callWithTwoArgs(String methodName, String arg1, String arg2) {
    if (GET_FROM_STEP.equals(methodName)) {
      return getFromStep(arg1, arg2);
    } else if (GET_FROM_SIGNAL.equals(methodName)) {
      return getFromSignal(arg1, arg2);
    } else if (GET_FROM_SIGNAL_DEPENDENCY.equals(methodName)) {
      return getFromSignalDependency(arg1, arg2);
    }
    throw new UnsupportedOperationException(
        String.format(
            "MaestroParamExtension DO NOT support calling method: %s with args: %s,%s",
            methodName, arg1, arg2));
  }

  @Override
  protected Object callWithThreeArgs(String methodName, String arg1, String arg2, String arg3) {
    if (GET_FROM_FOREACH.equals(methodName)) {
      Object res = getFromForeach(arg1, arg2, arg3);
      if (res.getClass().isArray()) {
        return res;
      } else {
        throw new IllegalStateException(
            methodName + " must return an array instead of " + res.getClass());
      }
    } else if (GET_FROM_SIGNAL_OR_DEFAULT.equals(methodName)) {
      return getFromSignalOrDefault(arg1, arg2, arg3);
    }
    throw new UnsupportedOperationException(
        String.format(
            "MaestroParamExtension DO NOT support calling method: %s with args: %s,%s,%s",
            methodName, arg1, arg2, arg3));
  }

  Object getFromStep(String stepId, String paramName) {
    try {
      return executor
          .submit(() -> fromStep(stepId, paramName))
          .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new MaestroInternalError(
          e, "getFromStep throws an exception for stepId=[%s], paramName=[%s]", stepId, paramName);
    }
  }

  Object getFromStep(String fieldName) {
    try {
      return executor
          .submit(() -> fromStep(fieldName))
          .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new MaestroInternalError(
          e, "getFromStep throws an exception fieldName=[%s]", fieldName);
    }
  }

  private Object fromStep(String stepId, String paramName) {
    StepRuntimeSummary runtimeSummary = validateAndGet(stepId);
    if (Constants.STEP_STATUS_PARAM.equals(paramName)) {
      return runtimeSummary.getRuntimeState().getStatus().name();
    }
    Parameter stepParam =
        Checks.notNull(
            runtimeSummary.getParams().get(paramName),
            "Cannot find the referenced param name [%s] in step [%s]",
            paramName,
            stepId);

    Checks.checkTrue(
        stepParam.isEvaluated(),
        "Referenced param [%s] in step [%s] is not evaluated yet. "
            + "Note that a step can only reference its upstream step's param.",
        stepParam,
        stepId);

    return stepParam.getEvaluatedResult();
  }

  private StepRuntimeSummary validateAndGet(String stepId) {
    Map<String, Object> stepData =
        Checks.notNull(
            allStepOutputData.get(stepId),
            "Cannot find the referenced step id [%s] in the current workflow",
            stepId);
    return StepHelper.retrieveRuntimeSummary(objectMapper, stepData);
  }

  Object getFromSignal(String signalName, String paramName) {
    try {
      return executor
          .submit(() -> fromSignal(signalName, paramName))
          .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new MaestroInternalError(
          e,
          "getFromSignal throws an exception for signalName=[%s], paramName=[%s]",
          signalName,
          paramName);
    }
  }

  // get signal or return a default string value if there is an exception.
  Object getFromSignalOrDefault(String signalName, String paramName, Object defaultValue) {
    try {
      return getFromSignal(signalName, paramName);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  private Object fromSignal(String signalName, String paramName) {
    Initiator initiator = instanceWrapper.getInitiator();
    if (initiator instanceof DryRunValidator.ValidationInitiator) {
      instanceWrapper.validateSignalName(signalName);
      // Note that it only works for string, long, or double type param
      return DUMMY_VALIDATION_VALUE;
    }
    Checks.checkTrue(
        initiator.getType() == Initiator.Type.SIGNAL,
        "Initiator [%s] is not a signal trigger and cannot get a param [%s] from it [%s]",
        initiator,
        paramName,
        signalName);
    Map<String, Parameter> params =
        Checks.notNull(
            ((SignalInitiator) initiator).getParams(),
            "Cannot get param [%s] from signal [%s] as signal initiator's param is null",
            paramName,
            signalName);
    Parameter signalParams =
        Checks.notNull(
            params.get(signalName),
            "Cannot find signal [%s] in the signal initiator[%s]",
            signalName,
            initiator);

    if (signalParams.getType() == ParamType.MAP) {
      return Checks.notNull(
          signalParams.asMap().get(paramName),
          "Cannot find param [%s] from the signal [%s]",
          paramName,
          signalName);
    } else if (signalParams.getType() == ParamType.STRING_MAP) {
      return Checks.notNull(
          signalParams.asStringMap().get(paramName),
          "Cannot find string param [%s] from the signal [%s]",
          paramName,
          signalName);
    } else {
      throw new MaestroValidationException(
          "Invalid param type [%s] for signal params [%s], which must be MAP or STRING_MAP",
          signalParams.getType(), signalName);
    }
  }

  Object getFromSignalDependency(String signalDependencyName, String paramName) {
    try {
      return executor
          .submit(() -> fromSignalDependency(signalDependencyName, paramName))
          .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new MaestroInternalError(
          e,
          "getFromSignalDependency throws an exception for signalDependencyName=[%s], paramName=[%s]",
          signalDependencyName,
          paramName);
    }
  }

  private Object fromSignalDependency(String signalDependencyName, String paramName) {
    List<Map<String, Parameter>> dependencyParams =
        Checks.notNull(
            signalDependenciesParams.get(signalDependencyName),
            "Cannot find signal dependency [%s] in the step signal dependencies [%s]",
            signalDependencyName,
            signalDependenciesParams.keySet());

    if (!dependencyParams.isEmpty()) {
      Checks.checkTrue(
          dependencyParams.size() == 1,
          "Not support referencing param [%s] from multiple signal dependencies with the same name [%s]",
          paramName,
          signalDependencyName);

      Parameter parameter = dependencyParams.get(0).get(paramName);
      if (parameter != null) {
        return parameter.getEvaluatedResult();
      }
    }
    throw new MaestroValidationException(
        "Cannot find param [%s] from signal dependency [%s]", paramName, signalDependencyName);
  }

  Object getFromForeach(String foreachStepId, String stepId, String paramName) {
    try {
      return executor
          .submit(() -> fromForeach(foreachStepId, stepId, paramName))
          .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new MaestroInternalError(
          e,
          "getFromForeach throws an exception for foreachStepId=[%s], stepId=[%s], paramName=[%s]",
          foreachStepId,
          stepId,
          paramName);
    }
  }

  private Object fromForeach(String foreachStepId, String stepId, String paramName) {
    StepRuntimeSummary runtimeSummary = validateAndGet(foreachStepId);
    Checks.checkTrue(
        runtimeSummary.getType() == StepType.FOREACH,
        "step [%s] is a type of [%s] instead of foreach step, cannot call getFromForeach",
        foreachStepId,
        runtimeSummary.getType());

    ForeachArtifact artifact =
        Checks.notNull(
                runtimeSummary.getArtifacts().get(Artifact.Type.FOREACH.key()),
                "Cannot load param [%s] of step [%s] from foreach [%s] as it is not initialized",
                paramName,
                stepId,
                foreachStepId)
            .asForeach();

    // collect data and put into an array without considering pagination first.
    ParamType type =
        stepInstanceDao.getForeachParamType(artifact.getForeachWorkflowId(), stepId, paramName);
    Map<Long, String> results =
        stepInstanceDao.getEvaluatedResultsFromForeach(
            artifact.getForeachWorkflowId(), stepId, paramName);
    int size = artifact.getTotalLoopCount();
    switch (type) {
      case STRING:
        String[] strArray = new String[size];
        results.forEach((key, value) -> strArray[Math.toIntExact(key) - 1] = value);
        return strArray;
      case LONG:
        long[] longArray = new long[size];
        results.forEach(
            (key, value) -> longArray[Math.toIntExact(key) - 1] = Long.parseLong(value));
        return longArray;
      case DOUBLE:
        double[] doubleArray = new double[size];
        results.forEach(
            (key, value) -> doubleArray[Math.toIntExact(key) - 1] = Double.parseDouble(value));
        return doubleArray;
      case BOOLEAN:
        boolean[] boolArray = new boolean[size];
        results.forEach(
            (key, value) -> boolArray[Math.toIntExact(key) - 1] = Boolean.parseBoolean(value));
        return boolArray;
      default:
        throw new UnsupportedOperationException(
            "cannot get param from foreach with a type: " + type);
    }
  }

  Long nextUniqueId() {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextInt(RANDOM_JITTER_DELAY));
      return executor
          .submit(stepInstanceDao::getNextUniqueId)
          .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new MaestroInternalError(e, "nextUniqueId throws an exception");
    }
  }

  Object getFromInstance(String fieldName) {
    try {
      Object ret =
          executor
              .submit(() -> fromInstance(fieldName))
              .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
      // As param does not allow null, using empty space indicates unset for now.
      return ret == null ? "" : ret;
    } catch (Exception e) {
      throw new MaestroInternalError(
          e, "getFromInstance throws an exception for fieldName=[%s]", fieldName);
    }
  }

  private Object fromInstance(String fieldName) {
    switch (fieldName) {
      case Constants.INITIATOR_TIMEZONE_PARAM:
        return instanceWrapper.getInitiatorTimeZone();
      case Constants.INITIATOR_TYPE_PARAM:
        if (instanceWrapper.getInitiator() instanceof DryRunValidator.ValidationInitiator) {
          return Constants.DRY_RUN_INITIATOR_TYPE_VALUE;
        }
        return instanceWrapper.getInitiator().getType().name();
      case Constants.INITIATOR_RUNNER_NAME:
        Checks.checkTrue(
            instanceWrapper.getInitiator().getType() == Initiator.Type.MANUAL,
            "Method [%s] (only for MANUAL run) does not support initiator [%s]",
            Constants.INITIATOR_RUNNER_NAME,
            instanceWrapper.getInitiator());
        return ((ManualInitiator) instanceWrapper.getInitiator()).getUser().getName();
      case Constants.WORKFLOW_RUN_POLICY_PARAM:
        return instanceWrapper.getRunPolicy();
      case Constants.WORKFLOW_OWNER_PARAM:
        Checks.checkTrue(
            instanceWrapper.isWorkflowParam(), GET_INSTANCE_FIELD_ERROR_MESSAGE, fieldName);
        return instanceWrapper.getWorkflowOwner();
      case Constants.FIRST_TIME_TRIGGER_TIMEZONE_PARAM:
        Checks.checkTrue(
            instanceWrapper.isWorkflowParam(), GET_INSTANCE_FIELD_ERROR_MESSAGE, fieldName);
        return instanceWrapper.getFirstTimeTriggerTimeZone();
      case Constants.WORKFLOW_ID_PARAM:
        return instanceWrapper.getWorkflowId();
      case Constants.WORKFLOW_INSTANCE_ID_PARAM:
        return instanceWrapper.getWorkflowInstanceId();
      default:
        throw new MaestroValidationException(
            "Invalid field name [%s] for getFromInstance call", fieldName);
    }
  }

  private Object fromStep(String fieldName) {
    Checks.checkTrue(
        !instanceWrapper.isWorkflowParam(),
        "fromStep [%s] is only available in step execution context",
        fieldName);
    StepInstanceAttributes stepInstanceAttributes =
        Checks.notNull(
            instanceWrapper.getStepInstanceAttributes(),
            "stepInstanceAttributes cannot be null [workflowId = %s fieldName = %s]",
            instanceWrapper.getWorkflowId(),
            fieldName);

    switch (fieldName) {
      case Constants.STEP_ID_PARAM:
        return stepInstanceAttributes.getStepId();
      case Constants.STEP_INSTANCE_ID_PARAM:
        return stepInstanceAttributes.getStepInstanceId();
      case Constants.STEP_INSTANCE_UUID_PARAM:
        return stepInstanceAttributes.getStepInstanceUuid();
      case Constants.STEP_TYPE_INFO_PARAM:
        return StepHelper.getStepTypeInfo(
            stepInstanceAttributes.getType(), stepInstanceAttributes.getSubType());
      default:
        throw new MaestroValidationException(
            "Invalid field name [%s] for getFromStep call", fieldName);
    }
  }
}
