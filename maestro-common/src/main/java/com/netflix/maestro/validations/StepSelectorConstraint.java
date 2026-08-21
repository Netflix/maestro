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
package com.netflix.maestro.validations;

import com.netflix.maestro.models.instance.StepSelector;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Validates a {@link StepSelector} at request time, so a bad selector is rejected instead of being
 * accepted and then failing every step of the run it was accepted for.
 */
@Documented
@Constraint(validatedBy = StepSelectorConstraint.StepSelectorValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface StepSelectorConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Step selector validator. */
  class StepSelectorValidator implements ConstraintValidator<StepSelectorConstraint, StepSelector> {
    /** Longest accepted step id pattern. Selecting steps needs nothing close to this. */
    private static final int PATTERN_LENGTH_LIMIT = 512;

    private static final String PATTERN_PREFIX = "[step selector] step_id_pattern [";

    /**
     * A quantifier applied to a group that itself contains a quantifier, such as {@code (a+)+} or
     * {@code (.*)*}. This is the shape whose backtracking is exponential in the input length, so it
     * can hang a step initialization thread on an input well within the step id length limit.
     * Matching a step id never needs it.
     */
    private static final Pattern NESTED_QUANTIFIER =
        Pattern.compile("\\((?:[^()\\\\]|\\\\.)*[*+}](?:[^()\\\\]|\\\\.)*\\)\\s*[*+]");

    @Override
    public void initialize(StepSelectorConstraint constraint) {}

    @Override
    public boolean isValid(StepSelector selector, ConstraintValidatorContext context) {
      if (selector == null || selector.getStepIdPattern() == null) {
        return true;
      }
      String pattern = selector.getStepIdPattern();
      if (pattern.length() > PATTERN_LENGTH_LIMIT) {
        return reject(
            context,
            "[step selector] step_id_pattern length ["
                + pattern.length()
                + "] is over the limit ["
                + PATTERN_LENGTH_LIMIT
                + "]");
      }
      if (NESTED_QUANTIFIER.matcher(pattern).find()) {
        return reject(
            context,
            PATTERN_PREFIX
                + pattern
                + "] nests a quantifier inside a quantified group, which can backtrack"
                + " exponentially. Use a flatter pattern or step_ids.");
      }
      try {
        Pattern.compile(pattern);
      } catch (PatternSyntaxException e) {
        return reject(
            context,
            PATTERN_PREFIX
                + pattern
                + "] is not a valid regular expression: "
                + e.getDescription());
      }
      return true;
    }

    private static boolean reject(ConstraintValidatorContext context, String message) {
      context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
      return false;
    }
  }
}
