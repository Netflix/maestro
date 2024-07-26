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

import com.netflix.maestro.models.definition.TagList;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Workflow version validation. */
@Documented
@Constraint(validatedBy = TagListConstraint.TagListValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface TagListConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro workflow validator. */
  class TagListValidator implements ConstraintValidator<TagListConstraint, TagList> {
    @Override
    public void initialize(TagListConstraint constraint) {}

    @Override
    public boolean isValid(TagList tags, ConstraintValidatorContext context) {
      if (tags != null && tags.containsDuplicate()) {
        context
            .buildConstraintViolationWithTemplate(
                "[tag list definition] cannot contain duplicate tag names")
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
