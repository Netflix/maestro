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
     * Escape sequences, replaced before scanning so a literal {@code \)} is not read as a group.
     */
    private static final Pattern ESCAPE_SEQUENCE = Pattern.compile("\\\\.");

    /**
     * A group repeated an unbounded or counted number of times, such as {@code (a+)+}, {@code
     * (a|ab)+}, {@code ((a)+)+} or {@code (a?b?)+}. Every regex whose backtracking is exponential
     * in the input length repeats a group, so rejecting that one shape covers the family rather
     * than enumerating its members. A step id well within the id length limit is enough to hang a
     * step initialization thread on any of them.
     *
     * <p>{@code ?} on a group is allowed, since at most one repetition cannot blow up, which keeps
     * patterns like {@code (load_)?users} usable. Matching a step id does not otherwise need a
     * repeated group; {@code step_ids} covers the cases where a literal sequence must repeat.
     */
    private static final Pattern REPEATED_GROUP = Pattern.compile("\\)[*+{]");

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
      if (REPEATED_GROUP.matcher(ESCAPE_SEQUENCE.matcher(pattern).replaceAll("x")).find()) {
        return reject(
            context,
            PATTERN_PREFIX
                + pattern
                + "] repeats a group, which can backtrack exponentially. Quantify a character or"
                + " character class instead, or list the steps in step_ids.");
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
