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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.utils.RetryPolicyParser;
import java.util.function.Function;
import org.junit.BeforeClass;
import org.junit.Test;

public class RetryPolicyParserTest extends MaestroBaseTest {
  private static final Function<ParamDefinition, Parameter> paramMapper =
      paramDefinition -> {
        Parameter param = paramDefinition.toParameter();
        param.setEvaluatedResult(param.getValue());
        param.setEvaluatedTime(1L);
        return param;
      };

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testGetParsedRetryNumberValid() throws Exception {
    RetryPolicy retries =
        loadObject("fixtures/retry_policy/sample-retries.json", RetryPolicy.class);
    RetryPolicy retryPolicy = RetryPolicyParser.getParsedRetryPolicy(retries, paramMapper);
    assertEquals(1L, retryPolicy.getErrorRetryLimit().getLong());
    assertEquals(2L, retryPolicy.getPlatformRetryLimit().getLong());
    assertEquals(3L, retryPolicy.getTimeoutRetryLimit().getLong());
  }

  @Test
  public void testGetParsedRetryNumberInvalid() throws Exception {
    RetryPolicy retries1 = RetryPolicy.builder().errorRetryLimit(ParsableLong.of(-1L)).build();
    AssertHelper.assertThrows(
        "Invalid retry number:",
        IllegalArgumentException.class,
        "cannot be negative or more than system limit: 99",
        () -> RetryPolicyParser.getParsedRetryPolicy(retries1, paramMapper));

    RetryPolicy retries2 = RetryPolicy.builder().errorRetryLimit(ParsableLong.of("bar")).build();
    AssertHelper.assertThrows(
        "Invalid retry number:",
        IllegalArgumentException.class,
        "error_retry_limit has an invalid value [bar]",
        () -> RetryPolicyParser.getParsedRetryPolicy(retries2, paramMapper));

    RetryPolicy retries3 = RetryPolicy.builder().errorRetryLimit(ParsableLong.of(1000L)).build();
    AssertHelper.assertThrows(
        "Invalid retry number:",
        IllegalArgumentException.class,
        "cannot be negative or more than system limit: 99",
        () -> RetryPolicyParser.getParsedRetryPolicy(retries3, paramMapper));
  }

  @Test
  public void testGetParsedRetryPolicyValid() {
    RetryPolicy parsedRetryPolicy =
        RetryPolicyParser.getParsedRetryPolicy(
            RetryPolicy.builder()
                .errorRetryLimit(ParsableLong.of(1L))
                .platformRetryLimit(ParsableLong.of("2"))
                .timeoutRetryLimit(ParsableLong.of(3L))
                .build(),
            paramMapper);

    RetryPolicy expected =
        RetryPolicy.builder()
            .errorRetryLimit(ParsableLong.of(1L))
            .platformRetryLimit(ParsableLong.of(2L))
            .timeoutRetryLimit(ParsableLong.of(3L))
            .build();
    assertEquals(expected, parsedRetryPolicy);
  }

  @Test
  public void testGetParsedRetryPolicyInvalid() {
    AssertHelper.assertThrows(
        "Should throw for invalid retry policy",
        NullPointerException.class,
        "because \"retryPolicy\" is null",
        () -> RetryPolicyParser.getParsedRetryPolicy(null, paramMapper));

    AssertHelper.assertThrows(
        "Should throw for invalid retry policy",
        IllegalArgumentException.class,
        "error_retry_limit value [-1] cannot be negative or more than system limit: 99",
        () ->
            RetryPolicyParser.getParsedRetryPolicy(
                RetryPolicy.builder().errorRetryLimit(ParsableLong.of(-1L)).build(), paramMapper));

    AssertHelper.assertThrows(
        "Should throw for invalid retry policy",
        IllegalArgumentException.class,
        "platform_retry_limit has an invalid value [unset]",
        () ->
            RetryPolicyParser.getParsedRetryPolicy(
                RetryPolicy.builder()
                    .errorRetryLimit(ParsableLong.of(1L))
                    .platformRetryLimit(ParsableLong.of("unset"))
                    .timeoutRetryLimit(ParsableLong.of(2L))
                    .build(),
                paramMapper));
  }

  @Test
  public void testGetParsedBackoffValid() {
    RetryPolicy parsed =
        RetryPolicyParser.getParsedRetryPolicy(RetryPolicy.builder().build(), paramMapper);
    assertNull(parsed.getBackoff());

    RetryPolicy retries1 =
        RetryPolicy.builder()
            .backoff(
                RetryPolicy.FixedBackoff.builder()
                    .errorRetryBackoffInSecs(ParsableLong.of(1L))
                    .platformRetryBackoffInSecs(ParsableLong.of("2"))
                    .timeoutRetryBackoffInSecs(ParsableLong.of(3L))
                    .build())
            .build();
    parsed = RetryPolicyParser.getParsedRetryPolicy(retries1, paramMapper);
    RetryPolicy.Backoff expected =
        RetryPolicy.FixedBackoff.builder()
            .errorRetryBackoffInSecs(ParsableLong.of(1L))
            .platformRetryBackoffInSecs(ParsableLong.of(2L))
            .timeoutRetryBackoffInSecs(ParsableLong.of(3L))
            .build();
    assertEquals(expected, parsed.getBackoff());

    RetryPolicy retries2 =
        RetryPolicy.builder()
            .backoff(
                RetryPolicy.ExponentialBackoff.builder()
                    .errorRetryBackoffInSecs(ParsableLong.of(1L))
                    .errorRetryExponent(ParsableLong.of(2L))
                    .errorRetryLimitInSecs(ParsableLong.of(10L))
                    .platformRetryBackoffInSecs(ParsableLong.of("2"))
                    .platformRetryExponent(ParsableLong.of("3"))
                    .platformRetryLimitInSecs(ParsableLong.of(15L))
                    .timeoutRetryBackoffInSecs(ParsableLong.of(3L))
                    .timeoutRetryExponent(ParsableLong.of(4L))
                    .timeoutRetryLimitInSecs(ParsableLong.of(100L))
                    .build())
            .build();
    parsed = RetryPolicyParser.getParsedRetryPolicy(retries2, paramMapper);
    expected =
        RetryPolicy.ExponentialBackoff.builder()
            .errorRetryBackoffInSecs(ParsableLong.of(1L))
            .errorRetryExponent(ParsableLong.of(2L))
            .errorRetryLimitInSecs(ParsableLong.of(10L))
            .platformRetryBackoffInSecs(ParsableLong.of(2L))
            .platformRetryExponent(ParsableLong.of(3L))
            .platformRetryLimitInSecs(ParsableLong.of(15L))
            .timeoutRetryBackoffInSecs(ParsableLong.of(3L))
            .timeoutRetryExponent(ParsableLong.of(4L))
            .timeoutRetryLimitInSecs(ParsableLong.of(100L))
            .build();
    assertEquals(expected, parsed.getBackoff());
  }

  @Test
  public void testGetParsedBackoffInvalid() {
    RetryPolicy retries1 =
        RetryPolicy.builder()
            .backoff(
                RetryPolicy.FixedBackoff.builder()
                    .errorRetryBackoffInSecs(
                        ParsableLong.of(Constants.MAX_ERROR_RETRY_LIMIT_SECS + 1))
                    .build())
            .build();
    AssertHelper.assertThrows(
        "Invalid retry number:",
        IllegalArgumentException.class,
        "backoff.error_retry_backoff_in_secs value [86401] cannot be negative or more than system limit: 86400",
        () -> RetryPolicyParser.getParsedRetryPolicy(retries1, paramMapper));

    RetryPolicy retries2 =
        RetryPolicy.builder()
            .backoff(
                RetryPolicy.FixedBackoff.builder()
                    .errorRetryBackoffInSecs(ParsableLong.of("bar"))
                    .build())
            .build();
    AssertHelper.assertThrows(
        "Invalid retry number:",
        IllegalArgumentException.class,
        "backoff.error_retry_backoff_in_secs has an invalid value [bar]",
        () -> RetryPolicyParser.getParsedRetryPolicy(retries2, paramMapper));
  }
}
