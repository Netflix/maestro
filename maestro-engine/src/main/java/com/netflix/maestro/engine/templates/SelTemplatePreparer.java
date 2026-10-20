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

import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;

/**
 * Preparer whose expansion payload is a SEL expression over the template inputs. The runtime
 * evaluates the registered expression with the SEL evaluator at prepare time and parses the JSON
 * string it returns as the expansion.
 *
 * <p>Not implemented yet. A template step registered with this preparer fails at prepare time.
 */
public class SelTemplatePreparer implements TemplatePreparer {
  @Override
  public String type() {
    return TemplatePrepare.SEL;
  }

  @Override
  public PrepareResult prepare(PrepareContext context) {
    throw new MaestroUnprocessableEntityException(
        "Template step [%s] uses the sel preparer, which is not implemented yet",
        context.step().getId());
  }
}
