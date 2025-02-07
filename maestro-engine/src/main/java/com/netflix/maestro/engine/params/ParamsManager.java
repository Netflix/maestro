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
package com.netflix.maestro.engine.params;

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepDependenciesDefinition;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.AbstractParamDefinition;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.utils.MapHelper;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** ParamsManager manages Parameter merging and cleanup. */
@AllArgsConstructor
@Slf4j
@SuppressWarnings({"checkstyle:OperatorWrap"})
public class ParamsManager {

  private final DefaultParamManager defaultParamManager;

  /**
   * Generate merged workflow parameters.
   *
   * @param instance workflow instance including run properties
   * @param request run request
   * @return a map of parameters by merging all the info
   */
  public Map<String, Parameter> generateMergedWorkflowParams(
      WorkflowInstance instance, RunRequest request) {
    Workflow workflow = instance.getRuntimeWorkflow();
    Map<String, ParamDefinition> allParamDefs = new LinkedHashMap<>();
    Map<String, ParamDefinition> defaultWorkflowParams =
        defaultParamManager.getDefaultWorkflowParams();

    // merge workflow params for start
    if (request.isFreshRun()) {
      // merge default workflow params
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          defaultWorkflowParams,
          ParamsMergeHelper.MergeContext.workflowCreate(ParamSource.SYSTEM_DEFAULT, request));

      // merge defined workflow params
      if (workflow.getParams() != null) {
        ParamsMergeHelper.mergeParams(
            allParamDefs,
            workflow.getParams(),
            ParamsMergeHelper.MergeContext.workflowCreate(ParamSource.DEFINITION, request));
      }
    }

    // merge workflow params from previous instance for restart
    if (!request.isFreshRun() && instance.getParams() != null) {
      Map<String, ParamDefinition> previousParamDefs =
          instance.getParams().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toDefinition()));

      // remove reserved params, which should be injected again by the system.
      for (String paramName : Constants.RESERVED_PARAM_NAMES) {
        previousParamDefs.remove(paramName);
      }

      ParamsMergeHelper.mergeParams(
          allParamDefs,
          previousParamDefs,
          ParamsMergeHelper.MergeContext.workflowCreate(ParamSource.SYSTEM, false));
    }

    // merge run params
    if (request.getRunParams() != null) {
      ParamSource source = getParamSource(request.getInitiator(), request.isFreshRun());
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          request.getRunParams(),
          ParamsMergeHelper.MergeContext.workflowCreate(source, request));
    }

    // merge user provided restart run params
    getUserRestartParam(request)
        .ifPresent(
            userRestartParams -> {
              ParamSource source = getParamSource(request.getInitiator(), request.isFreshRun());
              ParamsMergeHelper.mergeParams(
                  allParamDefs,
                  userRestartParams,
                  ParamsMergeHelper.MergeContext.workflowCreate(source, request));
            });

    // cleanup any placeholder params and convert to params
    return ParamsMergeHelper.convertToParameters(ParamsMergeHelper.cleanupParams(allParamDefs));
  }

  /**
   * Merge parameters in step definition and step runtime.
   *
   * <p>It includes all runtime params from step definition and step runtime injection and
   * conditional params.
   */
  public Map<String, Parameter> generateMergedStepParams(
      WorkflowSummary workflowSummary,
      Step stepDefinition,
      StepRuntime stepRuntime,
      StepRuntimeSummary runtimeSummary) {
    Map<String, ParamDefinition> allParamDefs = new LinkedHashMap<>();

    // Start with default step level params if present
    Map<String, ParamDefinition> globalDefault = defaultParamManager.getDefaultStepParams();
    if (globalDefault != null) {
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          globalDefault,
          ParamsMergeHelper.MergeContext.stepCreate(ParamSource.SYSTEM_DEFAULT));
    }
    // Merge in injected params returned by step if present (template schema)
    Map<String, ParamDefinition> injectedParams =
        stepRuntime.injectRuntimeParams(workflowSummary, stepDefinition);
    maybeOverrideParamType(allParamDefs);
    if (injectedParams != null) {
      maybeOverrideParamType(injectedParams);
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          injectedParams,
          ParamsMergeHelper.MergeContext.stepCreate(ParamSource.TEMPLATE_SCHEMA));
    }
    // Merge in params applicable to step type
    Optional<Map<String, ParamDefinition>> defaultStepTypeParams =
        defaultParamManager.getDefaultParamsForType(stepDefinition.getType());
    if (defaultStepTypeParams.isPresent()) {
      LOG.debug("Merging step level default for {}", stepDefinition.getType());
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          defaultStepTypeParams.get(),
          ParamsMergeHelper.MergeContext.stepCreate(ParamSource.SYSTEM_DEFAULT));
    }
    // Merge in workflow and step info
    ParamsMergeHelper.mergeParams(
        allParamDefs,
        injectWorkflowAndStepInfoParams(workflowSummary, runtimeSummary),
        ParamsMergeHelper.MergeContext.stepCreate(ParamSource.SYSTEM_INJECTED));

    // merge step run param and user provided restart step run params
    // first to get undefined params from both run param and restart params
    Map<String, ParamDefinition> undefinedRestartParams = new LinkedHashMap<>();
    Optional<Map<String, ParamDefinition>> stepRestartParams =
        getUserStepRestartParam(workflowSummary, runtimeSummary);
    stepRestartParams.ifPresent(undefinedRestartParams::putAll);
    Optional<Map<String, ParamDefinition>> stepRunParams =
        getStepRunParams(workflowSummary, runtimeSummary);

    Map<String, ParamDefinition> systemInjectedRestartRunParams = new LinkedHashMap<>();
    stepRunParams.ifPresent(
        params -> {
          params.forEach(
              (key, val) -> {
                if (runtimeSummary.getRestartConfig() != null
                    && Constants.RESERVED_PARAM_NAMES.contains(key)
                    && val.getMode() == ParamMode.CONSTANT
                    && val.getSource() == ParamSource.SYSTEM_INJECTED) {
                  ((AbstractParamDefinition) val)
                      .getMeta()
                      .put(Constants.METADATA_SOURCE_KEY, ParamSource.RESTART.name());
                  systemInjectedRestartRunParams.put(key, val);
                }
              });
          systemInjectedRestartRunParams.keySet().forEach(params::remove);
        });

    stepRunParams.ifPresent(undefinedRestartParams::putAll);

    Optional.ofNullable(stepDefinition.getParams())
        .ifPresent(
            stepDefParams ->
                stepDefParams.keySet().stream()
                    .filter(undefinedRestartParams::containsKey)
                    .forEach(undefinedRestartParams::remove));

    // Then merge undefined restart params
    if (!undefinedRestartParams.isEmpty()) {
      mergeUserProvidedStepParams(allParamDefs, undefinedRestartParams, workflowSummary);
    }

    // Final merge from step definition
    if (stepDefinition.getParams() != null) {
      maybeOverrideParamType(stepDefinition.getParams());
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          stepDefinition.getParams(),
          ParamsMergeHelper.MergeContext.stepCreate(ParamSource.DEFINITION));
    }

    // merge step run params
    stepRunParams.ifPresent(
        stepParams -> mergeUserProvidedStepParams(allParamDefs, stepParams, workflowSummary));

    // merge all user provided restart step run params
    stepRestartParams.ifPresent(
        stepParams -> mergeUserProvidedStepParams(allParamDefs, stepParams, workflowSummary));

    // merge all system injected restart step run params with mode and source already set.
    allParamDefs.putAll(systemInjectedRestartRunParams);

    // Cleanup any params that are missing and convert to params
    return ParamsMergeHelper.convertToParameters(ParamsMergeHelper.cleanupParams(allParamDefs));
  }

  /**
   * Override param type if needed. This is mainly used to help data mode change for backward
   * compatibility.
   *
   * @param params params to be overridden
   */
  protected void maybeOverrideParamType(Map<String, ParamDefinition> params) {}

  private void mergeUserProvidedStepParams(
      Map<String, ParamDefinition> allParamDefs,
      Map<String, ParamDefinition> userParams,
      WorkflowSummary workflowSummary) {
    RunPolicy runConfig = workflowSummary.getRunPolicy();
    boolean isFreshRun = runConfig == null || runConfig.isFreshRun();
    ParamSource source = getParamSource(workflowSummary.getInitiator(), isFreshRun);
    ParamsMergeHelper.mergeParams(
        allParamDefs,
        userParams,
        new ParamsMergeHelper.MergeContext(source, false, false, !isFreshRun));
  }

  /**
   * Generate static workflow parameter definition. This is for workflow definition and mainly used
   * for enrichment.
   *
   * @param workflow workflow definition
   * @return a map of parameters
   */
  public Map<String, ParamDefinition> generatedStaticWorkflowParamDefs(Workflow workflow) {
    Map<String, ParamDefinition> allParamDefs = new LinkedHashMap<>();
    Map<String, ParamDefinition> defaultWorkflowParams =
        defaultParamManager.getDefaultWorkflowParams();
    // merge default workflow params
    ParamsMergeHelper.mergeParams(
        allParamDefs,
        defaultWorkflowParams,
        ParamsMergeHelper.MergeContext.workflowCreate(ParamSource.SYSTEM_DEFAULT, false));

    // merge defined workflow params
    if (workflow.getParams() != null) {
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          workflow.getParams(),
          ParamsMergeHelper.MergeContext.workflowCreate(ParamSource.DEFINITION, false));
    }
    return allParamDefs;
  }

  /**
   * Generate static merged step params. This is for workflow and step definition and mainly used
   * for enrichment.
   *
   * @param workflowSummary workflow summary
   * @param stepDefinition step definition
   * @param stepRuntime step runtime
   * @return a map of parameters
   */
  public Map<String, ParamDefinition> generateStaticStepParamDefs(
      WorkflowSummary workflowSummary, Step stepDefinition, StepRuntime stepRuntime) {
    Map<String, ParamDefinition> allParamDefs = new LinkedHashMap<>();

    // Start with default step level params if present
    Map<String, ParamDefinition> globalDefault = defaultParamManager.getDefaultStepParams();

    if (globalDefault != null) {
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          globalDefault,
          ParamsMergeHelper.MergeContext.stepCreate(ParamSource.SYSTEM_DEFAULT));
    }
    // Merge in injected params returned by step if present (template schema)
    Map<String, ParamDefinition> injectedParams =
        stepRuntime.injectRuntimeParams(workflowSummary, stepDefinition);
    if (injectedParams != null) {
      maybeOverrideParamType(injectedParams);
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          injectedParams,
          ParamsMergeHelper.MergeContext.stepCreate(ParamSource.TEMPLATE_SCHEMA));
    }
    // Merge in params applicable to step type
    Optional<Map<String, ParamDefinition>> defaultStepTypeParams =
        defaultParamManager.getDefaultParamsForType(stepDefinition.getType());
    defaultStepTypeParams.ifPresent(
        stepTypeParams ->
            ParamsMergeHelper.mergeParams(
                allParamDefs,
                stepTypeParams,
                ParamsMergeHelper.MergeContext.stepCreate(ParamSource.SYSTEM_DEFAULT)));

    // Final merge from step definition
    if (stepDefinition.getParams() != null) {
      maybeOverrideParamType(stepDefinition.getParams());
      ParamsMergeHelper.mergeParams(
          allParamDefs,
          stepDefinition.getParams(),
          ParamsMergeHelper.MergeContext.stepCreate(ParamSource.DEFINITION));
    }
    return allParamDefs;
  }

  private ParamSource getParamSource(Initiator initiator, boolean isFreshRun) {
    if (initiator instanceof UpstreamInitiator) {
      return ((UpstreamInitiator) initiator).getParameterSource();
    } else {
      return isFreshRun ? ParamSource.LAUNCH : ParamSource.RESTART;
    }
  }

  private Optional<Map<String, ParamDefinition>> getStepRunParams(
      WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    if (runtimeSummary.getStepRunParams() != null) {
      return Optional.of(runtimeSummary.getStepRunParams());
    } else if (workflowSummary.getStepRunParams() != null
        && workflowSummary.getStepRunParams().containsKey(runtimeSummary.getStepId())) {
      return Optional.of(workflowSummary.getStepRunParams().get(runtimeSummary.getStepId()));
    } else {
      return Optional.empty();
    }
  }

  private Optional<Map<String, ParamDefinition>> getUserRestartParam(RunRequest runRequest) {
    RestartConfig restartConfig = runRequest.getRestartConfig();
    if (restartConfig != null
        && restartConfig.getRestartPath() != null
        && restartConfig.getRestartPath().size() == 1) {
      return Optional.ofNullable(restartConfig.getRestartParams());
    }
    return Optional.empty();
  }

  private Optional<Map<String, ParamDefinition>> getUserStepRestartParam(
      WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    RestartConfig stepRestartConfig =
        ObjectHelper.valueOrDefault(
            runtimeSummary.getRestartConfig(), workflowSummary.getRestartConfig());
    String stepId = runtimeSummary.getStepId();
    // If the path size is 1, the current one is the leaf/last node in the restart chain
    if (stepRestartConfig != null
        && stepRestartConfig.getRestartPath() != null
        && stepRestartConfig.getRestartPath().size() == 1
        && stepRestartConfig.getStepRestartParams() != null) {
      return Optional.ofNullable(stepRestartConfig.getStepRestartParams().get(stepId));
    }
    return Optional.empty();
  }

  /**
   * Inject system params based on workflow and step info. Override it if there are additional
   * system params to inject.
   */
  protected Map<String, ParamDefinition> injectWorkflowAndStepInfoParams(
      WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    return null;
  }

  /**
   * Helper method to create Step dependencies Params.
   *
   * @param dependencies definition of step dependencies
   * @return a map of dependency params
   */
  public static Map<StepDependencyType, List<MapParameter>> getStepDependenciesParameters(
      Collection<StepDependenciesDefinition> dependencies) {
    if (ObjectHelper.isCollectionEmptyOrNull(dependencies)) {
      return Collections.emptyMap();
    }
    return dependencies.stream()
        .collect(
            MapHelper.toListMap(
                StepDependenciesDefinition::getType,
                e ->
                    e.getDefinitions().stream()
                        .map(v -> (MapParameter) v.toParameter())
                        .collect(Collectors.toList())));
  }

  /**
   * Helper method to create Step output Params.
   *
   * @param outputs definition of step output
   * @return a map of output in map param format
   */
  public static Map<StepOutputsDefinition.StepOutputType, List<MapParameter>>
      getStepOutputsParameters(Collection<StepOutputsDefinition> outputs) {
    if (ObjectHelper.isCollectionEmptyOrNull(outputs)) {
      return new LinkedHashMap<>();
    }
    return outputs.stream()
        .collect(
            MapHelper.toListMap(
                StepOutputsDefinition::getType,
                e ->
                    e.asSignalOutputsDefinition().getDefinitions().stream()
                        .map(v -> (MapParameter) v.toParameter())
                        .collect(Collectors.toList())));
  }
}
