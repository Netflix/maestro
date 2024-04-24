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

import com.netflix.maestro.models.definition.Metadata;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

/** Workflow metadata validation. */
@Documented
@Constraint(validatedBy = MetadataConstraint.MetadataValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface MetadataConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro metadata validator. */
  class MetadataValidator implements ConstraintValidator<MetadataConstraint, Metadata> {

    @Override
    public void initialize(MetadataConstraint constraint) {}

    @Override
    public boolean isValid(Metadata metadata, ConstraintValidatorContext context) {
      if (metadata == null) {
        context
            .buildConstraintViolationWithTemplate("[workflow metadata] cannot be null")
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
