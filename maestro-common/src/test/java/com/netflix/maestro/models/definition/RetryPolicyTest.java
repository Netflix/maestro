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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class RetryPolicyTest extends MaestroBaseTest {
  private static Long CONFIGURED_PLATFORM_RETRY_LIMIT_SECS = 3L;
  private static Long CONFIGURED_ERROR_RETRY_LIMIT_SECS = 3L;
  private static Long RANDOM_ERROR_LIMIT = 10L;
  private static Long PLATFORM_RETRY_DELAY_SECS = 1L;
  private static Long ERROR_RETRY_DELAY_SECS = 1L;

  @Test
  public void testRoundTripSerdeExponentialBackoffDelay() throws Exception {
    RetryPolicy retryPolicy =
        loadObject(
            "fixtures/retry_policy/sample-retry-exponential-back-off-policy.json",
            RetryPolicy.class);
    assertEquals(
        retryPolicy, MAPPER.readValue(MAPPER.writeValueAsString(retryPolicy), RetryPolicy.class));
    assertEquals(
        RetryPolicy.BackoffPolicyType.EXPONENTIAL_BACKOFF, retryPolicy.getBackoff().getType());
    assertEquals(
        CONFIGURED_PLATFORM_RETRY_LIMIT_SECS,
        ((RetryPolicy.ExponentialBackoff) retryPolicy.getBackoff()).getErrorRetryLimitInSecs());
    assertEquals(
        CONFIGURED_ERROR_RETRY_LIMIT_SECS,
        ((RetryPolicy.ExponentialBackoff) retryPolicy.getBackoff()).getPlatformRetryLimitInSecs());
  }

  @Test
  public void testRoundTripSerdeFixedDelay() throws Exception {
    RetryPolicy retryPolicy =
        loadObject("fixtures/retry_policy/sample-retry-fixed-delay-policy.json", RetryPolicy.class);
    assertEquals(
        retryPolicy, MAPPER.readValue(MAPPER.writeValueAsString(retryPolicy), RetryPolicy.class));
    assertEquals(RetryPolicy.BackoffPolicyType.FIXED_BACKOFF, retryPolicy.getBackoff().getType());
    assertEquals(
        ERROR_RETRY_DELAY_SECS,
        ((RetryPolicy.FixedBackoff) retryPolicy.getBackoff()).getErrorRetryBackoffInSecs());
    assertEquals(
        PLATFORM_RETRY_DELAY_SECS,
        ((RetryPolicy.FixedBackoff) retryPolicy.getBackoff()).getPlatformRetryBackoffInSecs());
  }

  @Test
  public void testNoBackoff() throws Exception {
    RetryPolicy retryPolicy =
        loadObject(
            "fixtures/retry_policy/sample-retry-exponential-no-backoff.json", RetryPolicy.class);
    assertNull(retryPolicy.getBackoff());
  }

  @Test
  public void testRandomValues() throws Exception {
    RetryPolicy retryPolicy =
        loadObject("fixtures/retry_policy/sample-retry-random-values.json", RetryPolicy.class);
    assertNull(retryPolicy.getPlatformRetryLimit());
    assertNull(retryPolicy.getBackoff());
    assertEquals(RANDOM_ERROR_LIMIT, retryPolicy.getErrorRetryLimit());
  }

  @Test
  public void testOnlyBackoff() throws Exception {
    RetryPolicy retryPolicy =
        loadObject("fixtures/retry_policy/sample-retry-only-backoff.json", RetryPolicy.class);
    assertNull(retryPolicy.getPlatformRetryLimit());
    assertNull(retryPolicy.getErrorRetryLimit());
    assertEquals(RetryPolicy.BackoffPolicyType.FIXED_BACKOFF, retryPolicy.getBackoff().getType());
  }
}
