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
package com.netflix.maestro.engine.templates;

import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import java.util.LinkedHashMap;
import java.util.Map;

/** Run params a template step passes down to its inline instances. */
public final class TemplateParams {
  private TemplateParams() {}

  /**
   * The run params of an inline instance: every workflow param visible to the template step, then
   * every template param, registered or written in the step definition, with the value the template
   * step evaluated. Output params are published by the inline instance and are not passed down.
   *
   * @param context the template step being prepared
   * @return the run params, in a mutable map
   */
  public static Map<String, ParamDefinition> createRunParams(PrepareContext context) {
    Map<String, ParamDefinition> runParams = new LinkedHashMap<>();
    if (context.workflowSummary().getParams() != null) {
      context.workflowSummary().getParams().forEach((k, v) -> runParams.put(k, v.toDefinition()));
    }
    Map<String, Parameter> stepParams = context.runtimeSummary().getParams();
    JobTemplate.Definition definition = context.definition();
    if (definition.getParams() != null) {
      definition.getParams().keySet().forEach(k -> putStepParam(runParams, stepParams, k));
    }
    if (context.step().getParams() != null) {
      context.step().getParams().keySet().forEach(k -> putStepParam(runParams, stepParams, k));
    }
    if (definition.getOutputParams() != null) {
      definition.getOutputParams().keySet().forEach(runParams::remove);
    }
    return runParams;
  }

  private static void putStepParam(
      Map<String, ParamDefinition> runParams, Map<String, Parameter> stepParams, String name) {
    Parameter param = stepParams.get(name);
    if (param != null) {
      runParams.put(name, param.toDefinition());
    }
  }
}
