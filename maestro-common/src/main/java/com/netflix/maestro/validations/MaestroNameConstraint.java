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
import com.netflix.maestro.utils.StringUtils;
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

/** Maestro name validation, including tag name. */
@Documented
@Constraint(validatedBy = MaestroNameConstraint.MaestroNameValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface MaestroNameConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro id/name validator. */
  class MaestroNameValidator implements ConstraintValidator<MaestroNameConstraint, String> {
    private static final Pattern NAME_PATTERN =
        Pattern.compile("[\\w \\.,:@&='(){}$+\\[\\]\\-\\/\\\\]+");

    @Override
    public void initialize(MaestroNameConstraint constraint) {}

    @Override
    public boolean isValid(String id, ConstraintValidatorContext context) {
      if (id == null || StringUtils.checkTrimEmpty(id)) {
        context
            .buildConstraintViolationWithTemplate(
                "[maestro name] cannot be null or empty or blank spaces")
            .addConstraintViolation();
        return false;
      }

      if (id.length() > Constants.NAME_LENGTH_LIMIT) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[maestro name] cannot be more than name length limit %s "
                        + "- rejected length is [%s] for value [%s]",
                    Constants.NAME_LENGTH_LIMIT, id.length(), id))
            .addConstraintViolation();
        return false;
      }

      if (!NAME_PATTERN.matcher(id).matches()) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[maestro name] does not follow the regex rule: %s - rejected value is [%s]",
                    NAME_PATTERN.pattern(), id))
            .addConstraintViolation();
        return false;
      }

      if (id.toLowerCase(Locale.US).startsWith(Constants.MAESTRO_PREFIX)) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[maestro name] cannot start with reserved prefix: %s - rejected value is [%s]",
                    Constants.MAESTRO_PREFIX, id))
            .addConstraintViolation();
        return false;
      }

      if (id.toLowerCase(Locale.US).endsWith(Constants.MAESTRO_SUFFIX)) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[maestro name] cannot end with reserved suffix: %s - rejected value is [%s]",
                    Constants.MAESTRO_SUFFIX, id))
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
