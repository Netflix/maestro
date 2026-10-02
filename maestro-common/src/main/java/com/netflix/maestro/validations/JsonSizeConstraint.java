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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.utils.ExceptionHelper;
import com.netflix.maestro.utils.JsonHelper;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * JSON size validator. For example, {@code @JsonSizeConstraint("50")} sets a 50-byte limit and
 * {@code @JsonSizeConstraint("110.5KB")} sets a 110.5-kilobyte limit. MB and GB suffixes are also
 * supported.
 */
@Documented
@Constraint(validatedBy = JsonSizeConstraint.JsonSizeValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonSizeConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  String value();

  /** Maestro json size validator. */
  class JsonSizeValidator implements ConstraintValidator<JsonSizeConstraint, Object> {
    private static final int MAX_STACKTRACE_LINES = 20;
    private static final long BYTES_PER_KB = 1024L;
    private static final char KB = 'K';
    private static final char MB = 'M';
    private static final char GB = 'G';

    private String sizeLimit;
    private final ObjectMapper objectMapper = JsonHelper.objectMapper();

    @Override
    public void initialize(JsonSizeConstraint constraint) {
      sizeLimit = constraint.value();
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
      try (CountingOutputStream out = new CountingOutputStream()) {
        long sizeLimitBytes = parseSize(sizeLimit);
        objectMapper.writeValue(out, value);
        long wfCreateRequestNumBytes = out.getByteCount();
        if (wfCreateRequestNumBytes > sizeLimitBytes) {
          context
              .buildConstraintViolationWithTemplate(
                  String.format(
                      "Size of %s is %s bytes which is larger than limit of %s bytes",
                      value.getClass(), wfCreateRequestNumBytes, sizeLimitBytes))
              .addConstraintViolation();
          return false;
        }
        // This condition should never be hit since we are using the same objectMapper as the one
        // which was used to successfully deserialize the request at the controller level
      } catch (IOException e) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "Exception during size validation for %s with error=[%s] Type=[%s] StackTrace=[%s]",
                    value.getClass().getSimpleName(),
                    e.getMessage(),
                    e.getClass(),
                    ExceptionHelper.getStackTrace(e, MAX_STACKTRACE_LINES)))
            .addConstraintViolation();
        return false;
      } catch (NumberFormatException e) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "@JsonSizeConstraint(\"%s\") annotation is malformed. Check javadocs for "
                        + "JsonSizeConstraint annotation.",
                    sizeLimit))
            .addConstraintViolation();
        return false;
      }
      return true;
    }

    private static long parseSize(String text) {
      double size = Double.parseDouble(text.replaceAll("[GMK]B$", ""));
      char sizeChar = text.charAt(Math.max(0, text.length() - 2));
      if (sizeChar == KB) {
        size *= BYTES_PER_KB;
      } else if (sizeChar == MB) {
        size *= BYTES_PER_KB * BYTES_PER_KB;
      } else if (sizeChar == GB) {
        size *= BYTES_PER_KB * BYTES_PER_KB * BYTES_PER_KB;
      }
      return Math.round(size);
    }

    private static final class CountingOutputStream extends OutputStream {
      private long count;

      /** return the number of bytes that have passed through this stream. */
      public long getByteCount() {
        return count;
      }

      @Override
      public void write(final byte[] b, final int off, final int len) {
        count += len;
      }

      @Override
      public void write(final int b) {
        count++;
      }

      @Override
      public void write(final byte[] b) {
        int len = b == null ? 0 : b.length;
        count += len;
      }
    }
  }
}
