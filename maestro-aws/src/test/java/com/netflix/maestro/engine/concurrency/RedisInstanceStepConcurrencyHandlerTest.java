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
package com.netflix.maestro.engine.concurrency;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.redisson.api.RScript;

public class RedisInstanceStepConcurrencyHandlerTest extends MaestroBaseTest {

  @Mock private RScript redisLua;
  @Mock private Initiator initiator;

  private MaestroMetricRepo metricRepo;
  private RedisInstanceStepConcurrencyHandler handler;

  @Before
  public void before() throws Exception {
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    handler = new RedisInstanceStepConcurrencyHandler(redisLua, metricRepo);
  }

  @After
  public void tearDown() {
    metricRepo.reset();
  }

  @Test
  public void testRegisterStep() {
    assertEquals(Optional.empty(), handler.registerStep("foo", "bar"));
    verify(redisLua, times(1))
        .eval(
            RScript.Mode.READ_WRITE,
            "return redis.call('SADD', KEYS[1], ARGV[1]);",
            RScript.ReturnType.INTEGER,
            Collections.singletonList("{foo}:0"),
            "bar");

    when(redisLua.eval(any(), any(), any(), anyList(), any()))
        .thenThrow(new RuntimeException("test"));
    Optional<Details> actual = handler.registerStep("foo", "bar");
    assertTrue(actual.isPresent());
    assertEquals("Failed to register a step: bar", actual.get().getMessage());
    assertTrue(actual.get().isRetryable());
    assertEquals(Collections.singletonList("RuntimeException: test"), actual.get().getErrors());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
                RedisInstanceStepConcurrencyHandler.class,
                "type",
                "failedRegisterStep")
            .count());
  }

  @Test
  public void testUnregisterStep() {
    handler.unregisterStep("foo", "bar");
    verify(redisLua, times(1))
        .eval(
            RScript.Mode.READ_WRITE,
            "return redis.call('SREM', KEYS[1], ARGV[1]);",
            RScript.ReturnType.INTEGER,
            Collections.singletonList("{foo}:0"),
            "bar");

    when(redisLua.eval(any(), any(), any(), anyList(), any()))
        .thenThrow(new RuntimeException("test"));

    AssertHelper.assertThrows(
        "Unable to unregister the step",
        MaestroRetryableError.class,
        "Failed to unregister a step [bar] for concurrencyId [foo]",
        () -> handler.unregisterStep("foo", "bar"));
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
                RedisInstanceStepConcurrencyHandler.class,
                "type",
                "failedUnregisterStep")
            .count());
  }

  @Test
  public void testAddInstance() {
    RunRequest runRequest =
        RunRequest.builder()
            .initiator(initiator)
            .instanceStepConcurrency(10L)
            .requestId(new UUID(123, 456))
            .correlationId("foo")
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    when(initiator.getDepth()).thenReturn(2);
    when(redisLua.eval(any(), anyString(), any(), anyList(), anyString(), anyString()))
        .thenReturn(Boolean.TRUE);
    assertTrue(handler.addInstance(runRequest));

    verify(redisLua, times(1))
        .eval(
            eq(RScript.Mode.READ_WRITE),
            anyString(),
            eq(RScript.ReturnType.BOOLEAN),
            eq(Arrays.asList("{foo}:2", "{foo}:0")),
            eq("00000000-0000-007b-0000-0000000001c8"),
            eq("10"));

    when(redisLua.eval(any(), anyString(), any(), anyList(), anyString(), anyString()))
        .thenReturn(Boolean.FALSE);
    assertFalse(handler.addInstance(runRequest));

    verify(redisLua, times(2))
        .eval(
            eq(RScript.Mode.READ_WRITE),
            anyString(),
            eq(RScript.ReturnType.BOOLEAN),
            eq(Arrays.asList("{foo}:2", "{foo}:0")),
            eq("00000000-0000-007b-0000-0000000001c8"),
            eq("10"));

    when(redisLua.eval(any(), anyString(), any(), anyList(), anyString(), anyString()))
        .thenThrow(new RuntimeException("test"));
    assertFalse(handler.addInstance(runRequest));
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
                RedisInstanceStepConcurrencyHandler.class,
                "type",
                "failedAddInstance")
            .count());
  }

  @Test
  public void testRemoveInstance() {
    handler.removeInstance("foo", 3, "bar");
    verify(redisLua, times(1))
        .eval(
            RScript.Mode.READ_WRITE,
            "return redis.call('SREM', KEYS[1], ARGV[1]);",
            RScript.ReturnType.INTEGER,
            Collections.singletonList("{foo}:3"),
            "bar");

    when(redisLua.eval(any(), any(), any(), anyList(), any()))
        .thenThrow(new RuntimeException("test"));

    AssertHelper.assertThrows(
        "Unable to remove the instance",
        MaestroRetryableError.class,
        "Failed to remove instance for concurrency id [foo], depth [3], uuid [bar]",
        () -> handler.removeInstance("foo", 3, "bar"));
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
                RedisInstanceStepConcurrencyHandler.class,
                "type",
                "failedRemoveInstance")
            .count());
  }

  @Test
  public void testCleanUp() {
    handler.cleanUp("foo");
    verify(redisLua, times(1))
        .eval(
            RScript.Mode.READ_WRITE,
            "return redis.call('DEL', unpack(KEYS));",
            RScript.ReturnType.INTEGER,
            Arrays.asList(
                "{foo}:0", "{foo}:1", "{foo}:2", "{foo}:3", "{foo}:4", "{foo}:5", "{foo}:6",
                "{foo}:7", "{foo}:8", "{foo}:9"));

    when(redisLua.eval(any(), anyString(), any(), anyList()))
        .thenThrow(new RuntimeException("test"));

    AssertHelper.assertThrows(
        "Unable to cleanup the data",
        MaestroRetryableError.class,
        "Failed to cleanup all sets for [foo]",
        () -> handler.cleanUp("foo"));
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
                RedisInstanceStepConcurrencyHandler.class,
                "type",
                "failedCleanUp")
            .count());
  }
}
