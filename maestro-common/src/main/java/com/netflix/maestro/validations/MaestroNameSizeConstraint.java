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

import com.netflix.maestro.models.ValidationLimits;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Validates that a name field does not exceed the configurable name length limit. Unlike {@link
 * MaestroNameConstraint}, this annotation only enforces length and does not apply regex or reserved
 * prefix/suffix checks. It is null-safe (null is treated as absent and passes). It replaces
 * {@code @Size(max = Constants.NAME_LENGTH_LIMIT)} on optional model name fields where the limit
 * must be configurable at runtime.
 */
@Documented
@Constraint(validatedBy = MaestroNameSizeConstraint.MaestroNameSizeValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface MaestroNameSizeConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Validates that the string length does not exceed the name length limit. */
  class MaestroNameSizeValidator
      implements ConstraintValidator<MaestroNameSizeConstraint, String> {

    @Override
    public void initialize(MaestroNameSizeConstraint constraint) {}

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
      if (value == null) {
        return true; // null means absent; use @NotNull separately if the field is required
      }
      int limit = ValidationLimits.getNameLengthLimit();
      if (value.length() > limit) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[maestro name] cannot be more than name length limit %s"
                        + " - rejected length is [%s] for value [%s]",
                    limit, value.length(), value))
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
