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
package com.netflix.maestro.models.definition;

import static com.netflix.maestro.models.definition.StepDependenciesDefinition.STEP_DEPENDENCY_SUB_TYPE;

import com.netflix.maestro.models.parameter.MapParameter;
import java.util.Locale;

/** Various subtypes of step dependencies. */
public enum StepDependencySubType {
  /** Input Signal subtype. */
  INPUT_SIGNAL,

  /** Input Table subtype. */
  INPUT_TABLE,

  /** Input S3 subtype. */
  INPUT_S3;

  public static StepDependencySubType from(MapParameter mapParameter) {
    if (mapParameter.containsParam(STEP_DEPENDENCY_SUB_TYPE)) {
      String subType = mapParameter.getEvaluatedResultForParam(STEP_DEPENDENCY_SUB_TYPE);
      return valueOf(subType.toUpperCase(Locale.getDefault()));
    }
    return null;
  }
}
