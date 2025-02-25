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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.ParsableLong;
import com.netflix.maestro.models.definition.RetryPolicy;
import java.util.Set;
import javax.validation.ConstraintViolation;
import org.junit.Test;

public class RetryPolicyConstraintTest extends BaseConstraintTest {
  private static class ConstraintContainer {
    @RetryPolicyConstraint RetryPolicy retryPolicy;

    ConstraintContainer(
        ParsableLong errorRetryLimit,
        ParsableLong platformRetryLimit,
        ParsableLong timeoutRetryLimit,
        ParsableLong errorRetryBackoffInSecs,
        ParsableLong platformRetryBackoffInSecs,
        ParsableLong timeoutRetryBackoffInSecs) {
      this.retryPolicy =
          RetryPolicy.builder()
              .errorRetryLimit(errorRetryLimit)
              .platformRetryLimit(platformRetryLimit)
              .timeoutRetryLimit(timeoutRetryLimit)
              .backoff(
                  RetryPolicy.FixedBackoff.builder()
                      .errorRetryBackoffInSecs(errorRetryBackoffInSecs)
                      .platformRetryBackoffInSecs(platformRetryBackoffInSecs)
                      .timeoutRetryBackoffInSecs(timeoutRetryBackoffInSecs)
                      .build())
              .build();
    }
  }

  @Test
  public void testValid() {
    assertTrue(
        validator
            .validate(
                new ConstraintContainer(
                    ParsableLong.of(1L),
                    ParsableLong.of(2L),
                    ParsableLong.of(3L),
                    ParsableLong.of(4),
                    ParsableLong.of(5),
                    ParsableLong.of(6)))
            .isEmpty());
    assertTrue(
        validator
            .validate(
                new ConstraintContainer(
                    ParsableLong.of(10L),
                    ParsableLong.of("4"),
                    ParsableLong.of("0"),
                    ParsableLong.of(1),
                    ParsableLong.of(7),
                    ParsableLong.of(8)))
            .isEmpty());
    assertTrue(
        validator
            .validate(
                new ConstraintContainer(
                    ParsableLong.of("4"),
                    ParsableLong.of("${foo}"),
                    ParsableLong.of("0"),
                    ParsableLong.of("${bar}"),
                    ParsableLong.of("10"),
                    ParsableLong.of("2")))
            .isEmpty());
  }

  @Test
  public void testInvalid() {
    Set<ConstraintViolation<ConstraintContainer>> violations =
        validator.validate(
            new ConstraintContainer(
                ParsableLong.of(-1L),
                ParsableLong.of(Constants.MAX_RETRY_LIMIT + 1L),
                ParsableLong.of(1L),
                ParsableLong.of(1L),
                ParsableLong.of(1L),
                ParsableLong.of(1L)));
    assertEquals(1, violations.size());
    assertEquals(
        "RetryPolicy: error_retry_limit value [-1] is parsed to [-1] but cannot be less than [0] or more than [99]",
        violations.iterator().next().getMessage());

    violations =
        validator.validate(
            new ConstraintContainer(
                ParsableLong.of(1L),
                ParsableLong.of(Constants.MAX_RETRY_LIMIT + 1L),
                ParsableLong.of(1L),
                ParsableLong.of(1L),
                ParsableLong.of(1L),
                ParsableLong.of(1L)));
    assertEquals(1, violations.size());
    assertEquals(
        "RetryPolicy: platform_retry_limit value [100] is parsed to [100] but cannot be less than [0] or more than [99]",
        violations.iterator().next().getMessage());

    violations =
        validator.validate(
            new ConstraintContainer(
                ParsableLong.of("1"),
                ParsableLong.of("2"),
                ParsableLong.of("bar"),
                ParsableLong.of(1L),
                ParsableLong.of(1L),
                ParsableLong.of(1L)));
    assertEquals(
        "RetryPolicy: timeout_retry_limit value [\"bar\"] is parsed to [bar] but not a number",
        violations.iterator().next().getMessage());

    violations =
        validator.validate(
            new ConstraintContainer(
                ParsableLong.of(1L),
                ParsableLong.of(1L),
                ParsableLong.of(1L),
                ParsableLong.of(1L),
                ParsableLong.of("1"),
                ParsableLong.of(1L + Integer.MAX_VALUE)));
    assertEquals(
        "RetryPolicy: backoff.timeout_retry_backoff_in_secs value [2147483648] is parsed to [2147483648] but cannot be less than [1] or more than [86400]",
        violations.iterator().next().getMessage());
  }
}
