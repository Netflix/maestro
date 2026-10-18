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

import com.netflix.maestro.models.stepruntime.TemplateExpansion;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;

/** Preparer whose step list is stored in the registration. It is ready on the first call. */
public class StaticTemplatePreparer implements TemplatePreparer {
  @Override
  public String type() {
    return TemplatePrepare.STATIC;
  }

  @Override
  public PrepareResult prepare(PrepareContext context) {
    TemplatePrepare prepare = context.definition().getPrepare();
    Checks.notNull(
        prepare.getSteps(),
        "Job template [%s][%s] does not define any steps for the template step [%s]",
        context.definition().getJobType(),
        context.artifact().getTemplateVersion(),
        context.step().getId());
    TemplateExpansion expansion = new TemplateExpansion();
    expansion.setSteps(new ArrayList<>(prepare.getSteps()));
    return PrepareResult.ready(expansion, null);
  }
}
