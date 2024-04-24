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

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Optional;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

/** Maestro validation for user provided run parameter definition during restart or start. */
@Documented
@Constraint(validatedBy = RunParamsConstraint.ParamMapValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface RunParamsConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro id validator. */
  class ParamMapValidator implements ConstraintValidator<RunParamsConstraint, Map<?, ?>> {

    @Override
    public void initialize(RunParamsConstraint constraint) {}

    @Override
    public boolean isValid(Map<?, ?> paramMap, ConstraintValidatorContext context) {
      if (paramMap != null && !paramMap.isEmpty()) {

        if (paramMap.size() > Constants.PARAM_MAP_SIZE_LIMIT) {
          context
              .buildConstraintViolationWithTemplate(
                  "[param definition] contain the number of params ["
                      + paramMap.size()
                      + "] more than param size limit "
                      + Constants.PARAM_MAP_SIZE_LIMIT)
              .addConstraintViolation();
          return false;
        }

        Optional<String> rejectedFirst =
            Constants.RESERVED_PARAM_NAMES.stream().filter(paramMap::containsKey).findFirst();

        if (rejectedFirst.isPresent()) {
          context
              .buildConstraintViolationWithTemplate(
                  String.format(
                      "[param definition] cannot use any of reserved param names - rejected value is [%s]",
                      rejectedFirst.get()))
              .addConstraintViolation();
          return false;
        }

        for (Map.Entry<?, ?> entry : paramMap.entrySet()) {
          if (((ParamDefinition) entry.getValue()).getSource() != null) {
            context
                .buildConstraintViolationWithTemplate(
                    "[param definition] users cannot set param source for a param: "
                        + entry.getKey())
                .addConstraintViolation();
            return false;
          }
        }
      }

      return true;
    }
  }
}
