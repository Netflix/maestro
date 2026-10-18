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
import com.netflix.maestro.models.stepruntime.JobTemplate;
import java.util.Collections;
import java.util.Map;

/**
 * Produces the step list a template step runs. A job template registration names its preparer by
 * {@link com.netflix.maestro.models.stepruntime.TemplatePrepare#getType()}; the engine resolves the
 * matching implementation among the enabled preparers at run time.
 *
 * <p>A preparer is stateless. Everything it needs comes from the {@link PrepareContext}, and
 * everything it decides goes back through the {@link PrepareResult} or the context's artifact.
 */
public interface TemplatePreparer {
  /** The prepare type this preparer serves, matched against the registered prepare type. */
  String type();

  /**
   * Params to declare on the template step before it starts, in addition to the registered template
   * params and output params. They are evaluated with the other step params.
   *
   * @param definition the registered job template definition
   * @return params to inject, possibly empty
   */
  default Map<String, ParamDefinition> injectParams(JobTemplate.Definition definition) {
    return Collections.emptyMap();
  }

  /**
   * Produce the expansion, or report that it is still pending or has failed. The runtime calls it
   * on every execute() until the result is ready or failed.
   *
   * @param context the template step being prepared
   * @return the prepare result
   */
  PrepareResult prepare(PrepareContext context);
}
