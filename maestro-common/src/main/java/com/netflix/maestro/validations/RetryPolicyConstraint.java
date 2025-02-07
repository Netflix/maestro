/*
 * Copyright 2025 Netflix, Inc.
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

import com.netflix.maestro.models.definition.RetryPolicy;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.utils.RetryPolicyParser;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Function;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

/** Retry policy validation. Note that it won't be able to validate string interpolated retries. */
@Documented
@Constraint(validatedBy = RetryPolicyConstraint.RetryValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface RetryPolicyConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** RetryPolicy validator. */
  class RetryValidator implements ConstraintValidator<RetryPolicyConstraint, RetryPolicy> {
    private static final String DUMMY_EVALUATED_RESULT = "2";
    private static final Long DUMMY_EVALUATION_TIME = 1L;
    private static final Function<ParamDefinition, Parameter> IGNORE_INTERPOLATION_MAPPING =
        paramDefinition -> {
          Parameter param = paramDefinition.toParameter();
          param.setEvaluatedResult(
              param.getType() == ParamType.STRING && param.asStringParam().getValue().contains("$")
                  ? DUMMY_EVALUATED_RESULT
                  : param.getValue());
          param.setEvaluatedTime(DUMMY_EVALUATION_TIME);
          return param;
        };

    @Override
    public boolean isValid(RetryPolicy retryPolicy, ConstraintValidatorContext context) {
      if (retryPolicy == null) {
        return true;
      }
      try {
        RetryPolicyParser.getParsedRetryPolicy(retryPolicy, IGNORE_INTERPOLATION_MAPPING);
      } catch (IllegalArgumentException e) {
        context
            .buildConstraintViolationWithTemplate("RetryPolicy: " + e.getMessage())
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
