/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.engine.steps;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.utils.Checks;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;

/**
 * Step runtime for {@link StepType#EXTENSION} steps.
 *
 * <p>An extension step carries its concrete kind in its sub type. This runtime hands every call to
 * the runtime registered for that sub type in the {@link ExtensionStepRuntimeRegistry}. When the
 * sub type itself is not registered, the job template ancestors of the sub type are tried in order,
 * so a job template that inherits from a registered sub type runs on that sub type's runtime.
 *
 * <p>Like every step runtime it is stateless: the resolved runtime is looked up on each call.
 */
@AllArgsConstructor
public class ExtensionStepRuntime implements StepRuntime {
  private final ExtensionStepRuntimeRegistry registry;
  private final JobTemplateManager jobTemplateManager;

  @Override
  public Result start(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    return resolve(workflowSummary, step).start(workflowSummary, step, runtimeSummary);
  }

  @Override
  public Result execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    return resolve(workflowSummary, step).execute(workflowSummary, step, runtimeSummary);
  }

  @Override
  public Result terminate(WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    return resolve(workflowSummary, runtimeSummary.getSubType(), runtimeSummary.getSubTypeVersion())
        .terminate(workflowSummary, runtimeSummary);
  }

  @Override
  public Map<String, ParamDefinition> injectRuntimeParams(
      WorkflowSummary workflowSummary, Step step) {
    return resolve(workflowSummary, step).injectRuntimeParams(workflowSummary, step);
  }

  @Override
  public List<Tag> injectRuntimeTags(WorkflowSummary workflowSummary, Step step) {
    return resolve(workflowSummary, step).injectRuntimeTags(workflowSummary, step);
  }

  private StepRuntime resolve(WorkflowSummary workflowSummary, Step step) {
    return resolve(workflowSummary, step.getSubType(), step.getSubTypeVersion());
  }

  /**
   * Resolve the runtime for a sub type: the runtime registered for the sub type itself, otherwise
   * the first registered runtime among its job template ancestors.
   */
  private StepRuntime resolve(
      WorkflowSummary workflowSummary, String subType, @Nullable String subTypeVersion) {
    Checks.notNull(subType, "Extension step must define a sub type");
    StepRuntime runtime = registry.lookup(subType);
    if (runtime == null) {
      List<String> ancestors =
          jobTemplateManager.loadAncestorJobTypes(workflowSummary, subType, subTypeVersion);
      for (String ancestor : ancestors) {
        runtime = registry.lookup(ancestor);
        if (runtime != null) {
          break;
        }
      }
    }
    return Checks.notNull(
        runtime,
        "Cannot find an extension step runtime for sub type [%s] or its job template ancestors",
        subType);
  }
}
