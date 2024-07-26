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
package com.netflix.maestro.validations;

import com.netflix.maestro.models.definition.Alerting;
import jakarta.validation.ConstraintValidatorContext;

/** Interface for validating an {@link Alerting} object. */
public interface AlertingValidator {

  /**
   * Validates an alerting object.
   *
   * @param alerting alerting config
   * @param context context
   * @return whether the alerting object is valid
   */
  boolean isValid(Alerting alerting, ConstraintValidatorContext context);
}
