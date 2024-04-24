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
package com.netflix.maestro.models.instance;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.RetryPolicy;
import java.util.Arrays;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

public class StepInstanceTest extends MaestroBaseTest {

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    for (String fileName :
        Arrays.asList(
            "sample-step-instance-running.json",
            "sample-step-instance-succeeded.json",
            "sample-step-instance-finishing.json",
            "sample-step-instance-failed.json")) {
      StepInstance expected = loadObject("fixtures/instances/" + fileName, StepInstance.class);
      String ser1 = MAPPER.writeValueAsString(expected);
      StepInstance actual =
          MAPPER.readValue(MAPPER.writeValueAsString(expected), StepInstance.class);
      String ser2 = MAPPER.writeValueAsString(actual);
      Assertions.assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
      assertEquals(ser1, ser2);
    }
  }

  @Test
  public void testMergeRetryPolicy() {
    RetryPolicy.RetryPolicyBuilder policyBuilder = RetryPolicy.builder();
    StepInstance.StepRetry retry = StepInstance.StepRetry.from(policyBuilder.build());
    // All default values.
    assertEquals(
        Defaults.DEFAULT_RETRY_POLICY.getErrorRetryLimit().longValue(), retry.getErrorRetryLimit());
    assertEquals(
        Defaults.DEFAULT_RETRY_POLICY.getPlatformRetryLimit().longValue(),
        retry.getPlatformRetryLimit());
    assertEquals(RetryPolicy.BackoffPolicyType.EXPONENTIAL_BACKOFF, retry.getBackoff().getType());
    RetryPolicy.ExponentialBackoff ebackOff = (RetryPolicy.ExponentialBackoff) retry.getBackoff();
    assertEquals(
        Defaults.DEFAULT_EXPONENTIAL_BACK_OFF.getErrorRetryBackoffInSecs(),
        ebackOff.getErrorRetryBackoffInSecs());
    assertEquals(
        Defaults.DEFAULT_EXPONENTIAL_BACK_OFF.getErrorRetryLimitInSecs(),
        ebackOff.getErrorRetryLimitInSecs());
    assertEquals(
        Defaults.DEFAULT_EXPONENTIAL_BACK_OFF.getErrorRetryExponent(),
        ebackOff.getErrorRetryExponent());
    assertEquals(
        Defaults.DEFAULT_EXPONENTIAL_BACK_OFF.getPlatformRetryBackoffInSecs(),
        ebackOff.getPlatformRetryBackoffInSecs());
    assertEquals(
        Defaults.DEFAULT_EXPONENTIAL_BACK_OFF.getPlatformRetryLimitInSecs(),
        ebackOff.getPlatformRetryLimitInSecs());
    assertEquals(
        Defaults.DEFAULT_EXPONENTIAL_BACK_OFF.getPlatformRetryExponent(),
        ebackOff.getPlatformRetryExponent());
    // Randomly set some values for exponential backoff.
    RetryPolicy.ExponentialBackoff configuredBackoff =
        RetryPolicy.ExponentialBackoff.builder()
            .platformRetryLimitInSecs(360L)
            .errorRetryExponent(4)
            .errorRetryLimitInSecs(200L)
            .build();
    policyBuilder.backoff(configuredBackoff);
    retry = StepInstance.StepRetry.from(policyBuilder.build());
    assertEquals(retry.getBackoff().getType(), RetryPolicy.BackoffPolicyType.EXPONENTIAL_BACKOFF);
    ebackOff = (RetryPolicy.ExponentialBackoff) retry.getBackoff();
    assertEquals(
        ebackOff.getErrorRetryBackoffInSecs(),
        Defaults.DEFAULT_EXPONENTIAL_BACK_OFF.getErrorRetryBackoffInSecs());
    assertEquals(200L, ebackOff.getErrorRetryLimitInSecs().longValue());
    assertEquals(4, ebackOff.getErrorRetryExponent().intValue());
    assertEquals(
        ebackOff.getPlatformRetryBackoffInSecs(),
        Defaults.DEFAULT_EXPONENTIAL_BACK_OFF.getPlatformRetryBackoffInSecs());
    assertEquals(360L, ebackOff.getPlatformRetryLimitInSecs().longValue());
    assertEquals(
        ebackOff.getPlatformRetryExponent(),
        Defaults.DEFAULT_EXPONENTIAL_BACK_OFF.getPlatformRetryExponent());
    // Randomly set some values for fixed backoff.
    RetryPolicy.FixedBackoff configuredFixedBackoff =
        RetryPolicy.FixedBackoff.builder().errorRetryBackoffInSecs(12L).build();
    policyBuilder.backoff(configuredFixedBackoff);
    retry = StepInstance.StepRetry.from(policyBuilder.build());
    assertEquals(retry.getBackoff().getType(), RetryPolicy.BackoffPolicyType.FIXED_BACKOFF);
    RetryPolicy.FixedBackoff fixedBackoff = (RetryPolicy.FixedBackoff) retry.getBackoff();
    assertEquals(12L, fixedBackoff.getErrorRetryBackoffInSecs().longValue());
    assertEquals(
        Defaults.DEFAULT_FIXED_BACK_OFF.getPlatformRetryBackoffInSecs(),
        fixedBackoff.getPlatformRetryBackoffInSecs());
  }

  @Test
  public void testGetNextRetryDelayExponentialPlatformFailed() {
    StepInstance.StepRetry retry = new StepInstance.StepRetry();
    RetryPolicy.ExponentialBackoff eBackoff = Defaults.DEFAULT_EXPONENTIAL_BACK_OFF;
    retry.setBackoff(eBackoff);
    assertEquals(
        eBackoff.getPlatformRetryBackoffInSecs().intValue(),
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    assertEquals(
        eBackoff.getPlatformRetryBackoffInSecs() * 2L,
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    assertEquals(
        eBackoff.getPlatformRetryBackoffInSecs() * 4L,
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    assertEquals(
        eBackoff.getPlatformRetryBackoffInSecs() * 8L,
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    assertEquals(
        eBackoff.getPlatformRetryBackoffInSecs() * 16L,
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    assertEquals(
        eBackoff.getPlatformRetryBackoffInSecs() * 32L,
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    // Max limit reached for wait.
    assertEquals(
        eBackoff.getPlatformRetryLimitInSecs().intValue(),
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    // Max limit reached for wait.
    assertEquals(
        eBackoff.getPlatformRetryLimitInSecs().longValue(),
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
  }

  @Test
  public void testGetNextRetryDelayExponentialUserFailed() {
    StepInstance.StepRetry retry = new StepInstance.StepRetry();
    RetryPolicy.ExponentialBackoff eBackoff = Defaults.DEFAULT_EXPONENTIAL_BACK_OFF;
    retry.setBackoff(eBackoff);
    assertEquals(
        eBackoff.getErrorRetryBackoffInSecs().longValue(),
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
    retry.incrementByStatus(StepInstance.Status.USER_FAILED);
    assertEquals(
        eBackoff.getErrorRetryBackoffInSecs() * 2L,
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
    retry.incrementByStatus(StepInstance.Status.USER_FAILED);
    assertEquals(
        eBackoff.getErrorRetryBackoffInSecs() * 4L,
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
    retry.incrementByStatus(StepInstance.Status.USER_FAILED);
    assertEquals(
        eBackoff.getErrorRetryBackoffInSecs() * 8L,
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
    // Max limit reached for wait.
    retry.incrementByStatus(StepInstance.Status.USER_FAILED);
    assertEquals(
        eBackoff.getErrorRetryLimitInSecs().longValue(),
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
    // Max limit reached for wait.
    retry.incrementByStatus(StepInstance.Status.USER_FAILED);
    assertEquals(
        eBackoff.getErrorRetryLimitInSecs().longValue(),
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
  }

  @Test
  public void testGetNextRetryDelayFixedPlatformFailed() {
    StepInstance.StepRetry retry = new StepInstance.StepRetry();
    RetryPolicy.FixedBackoff fixedBackoff = Defaults.DEFAULT_FIXED_BACK_OFF;
    retry.setBackoff(fixedBackoff);
    assertEquals(
        fixedBackoff.getPlatformRetryBackoffInSecs().longValue(),
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    assertEquals(
        fixedBackoff.getPlatformRetryBackoffInSecs().longValue(),
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    assertEquals(
        fixedBackoff.getPlatformRetryBackoffInSecs().longValue(),
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    assertEquals(
        fixedBackoff.getPlatformRetryBackoffInSecs().longValue(),
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
    retry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    assertEquals(
        fixedBackoff.getPlatformRetryBackoffInSecs().longValue(),
        retry.getNextRetryDelay(StepInstance.Status.PLATFORM_FAILED));
  }

  @Test
  public void testGetNextRetryDelayFixedUserFailed() {
    StepInstance.StepRetry retry = new StepInstance.StepRetry();
    RetryPolicy.FixedBackoff fixedBackoff = Defaults.DEFAULT_FIXED_BACK_OFF;
    retry.setBackoff(fixedBackoff);
    assertEquals(
        fixedBackoff.getErrorRetryBackoffInSecs().intValue(),
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
    retry.incrementByStatus(StepInstance.Status.USER_FAILED);
    assertEquals(
        fixedBackoff.getErrorRetryBackoffInSecs().intValue(),
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
    retry.incrementByStatus(StepInstance.Status.USER_FAILED);
    assertEquals(
        fixedBackoff.getErrorRetryBackoffInSecs().intValue(),
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
    retry.incrementByStatus(StepInstance.Status.USER_FAILED);
    assertEquals(
        fixedBackoff.getErrorRetryBackoffInSecs().intValue(),
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
    retry.incrementByStatus(StepInstance.Status.USER_FAILED);
    assertEquals(
        fixedBackoff.getErrorRetryBackoffInSecs().intValue(),
        retry.getNextRetryDelay(StepInstance.Status.USER_FAILED));
  }

  @Test
  public void testGetNextRetryDelayNonRestartable() {
    StepInstance.StepRetry retry = new StepInstance.StepRetry();
    AssertHelper.assertThrows(
        "Not expected",
        MaestroInvalidStatusException.class,
        "Invalid status [COMPLETED_WITH_ERROR] to get next retry delay",
        () -> retry.getNextRetryDelay(StepInstance.Status.COMPLETED_WITH_ERROR));
  }
}
