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
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Locale;
import java.util.regex.Pattern;

/** Maestro reference id/name validation, including step id and param name. */
@Documented
@Constraint(validatedBy = MaestroReferenceIdConstraint.MaestroIdValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface MaestroReferenceIdConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro id/name validator. */
  class MaestroIdValidator implements ConstraintValidator<MaestroReferenceIdConstraint, String> {
    private static final Pattern ID_PATTERN = Pattern.compile("[_a-zA-Z][.\\-_a-zA-Z0-9]*+");
    private static final String REJECTED_VALUE = "- rejected value is [%s]";

    @Override
    public void initialize(MaestroReferenceIdConstraint constraint) {}

    @Override
    public boolean isValid(String id, ConstraintValidatorContext context) {
      if (id == null || id.isEmpty()) {
        context
            .buildConstraintViolationWithTemplate(
                "[maestro id or name reference] cannot be null or empty")
            .addConstraintViolation();
        return false;
      }

      if (id.length() > Constants.ID_LENGTH_LIMIT) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[maestro id or name reference] cannot be more than id length limit %s "
                        + "- rejected length is [%s] for value [%s]",
                    Constants.ID_LENGTH_LIMIT, id.length(), id))
            .addConstraintViolation();
        return false;
      }

      if (!ID_PATTERN.matcher(id).matches()) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[maestro id or name reference] does not follow the regex rule: %s "
                        + REJECTED_VALUE,
                    ID_PATTERN.pattern(),
                    id))
            .addConstraintViolation();
        return false;
      }

      if (id.contains("__")) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[maestro id or name reference] cannot contain double underscores '__' "
                        + REJECTED_VALUE,
                    id))
            .addConstraintViolation();
        return false;
      }

      if (id.toLowerCase(Locale.US).startsWith(Constants.MAESTRO_PREFIX)) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[maestro id or name reference] cannot start with reserved prefix: %s "
                        + REJECTED_VALUE,
                    Constants.MAESTRO_PREFIX,
                    id))
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
