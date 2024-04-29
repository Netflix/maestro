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
package com.netflix.maestro.engine.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.properties.CallbackDelayConfig;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class StepRuntimeFixedCallbackPolicyTest extends MaestroEngineBaseTest {
  @Mock private CallbackDelayConfig callbackDelayConfig;
  private StepRuntimeFixedCallbackDelayPolicy stepRuntimeFixedCallbackDelayPolicy;

  @Before
  public void before() {
    this.stepRuntimeFixedCallbackDelayPolicy =
        new StepRuntimeFixedCallbackDelayPolicy(callbackDelayConfig);
  }

  @Test
  public void testGetCallBackInSecs() {
    Map<StepInstance.Status, Map<String, Long>> stepRuntimeCallBackDelayConfig =
        ImmutableMap.of(
            StepInstance.Status.RUNNING,
            threeItemMap(
                StepType.TITUS.getType().toLowerCase(Locale.US),
                30L,
                StepType.NOTEBOOK.getType().toLowerCase(Locale.US),
                30L,
                "default",
                3L),
            StepInstance.Status.WAITING_FOR_SIGNALS,
            Collections.singletonMap("default", 60L),
            StepInstance.Status.WAITING_FOR_PERMITS,
            Collections.singletonMap("default", 80L),
            StepInstance.Status.PAUSED,
            Collections.singletonMap("default", 90L));
    when(callbackDelayConfig.getFixedCallbackDelayInSecs())
        .thenReturn(stepRuntimeCallBackDelayConfig);
    StepRuntimeState stepRuntimeState = new StepRuntimeState();
    stepRuntimeState.setStatus(StepInstance.Status.RUNNING);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder().runtimeState(stepRuntimeState).type(StepType.NOOP).build();
    Long callback = stepRuntimeFixedCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary);
    assertEquals(3L, callback.longValue());
    runtimeSummary =
        StepRuntimeSummary.builder().runtimeState(stepRuntimeState).type(StepType.TITUS).build();
    callback = stepRuntimeFixedCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary);
    assertEquals(30L, callback.longValue());
    runtimeSummary =
        StepRuntimeSummary.builder().runtimeState(stepRuntimeState).type(StepType.NOTEBOOK).build();
    callback = stepRuntimeFixedCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary);
    assertEquals(30L, callback.longValue());
    runtimeSummary.getRuntimeState().setStatus(StepInstance.Status.WAITING_FOR_SIGNALS);
    callback = stepRuntimeFixedCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary);
    assertEquals(60L, callback.longValue());
    runtimeSummary.getRuntimeState().setStatus(StepInstance.Status.WAITING_FOR_PERMITS);
    runtimeSummary =
        StepRuntimeSummary.builder().runtimeState(stepRuntimeState).type(StepType.NOOP).build();
    callback = stepRuntimeFixedCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary);
    assertEquals(80L, callback.longValue());
    runtimeSummary =
        StepRuntimeSummary.builder().runtimeState(stepRuntimeState).type(StepType.TITUS).build();
    assertEquals(80L, callback.longValue());
    runtimeSummary.getRuntimeState().setStatus(StepInstance.Status.STOPPED);
    callback = stepRuntimeFixedCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary);
    assertNull(callback);
    runtimeSummary.getRuntimeState().setStatus(StepInstance.Status.PAUSED);
    runtimeSummary =
        StepRuntimeSummary.builder().runtimeState(stepRuntimeState).type(StepType.NOOP).build();
    callback = stepRuntimeFixedCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary);
    assertEquals(90L, callback.longValue());
    runtimeSummary =
        StepRuntimeSummary.builder().runtimeState(stepRuntimeState).type(StepType.TITUS).build();
    callback = stepRuntimeFixedCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary);
    assertEquals(90L, callback.longValue());
  }

  @Test
  public void testGetCallBackInSecsInvalid() {
    Map<StepInstance.Status, Map<String, Long>> stepRuntimeCallBackDelayConfig =
        Collections.singletonMap(
            StepInstance.Status.RUNNING,
            Collections.singletonMap(StepType.TITUS.getType().toLowerCase(Locale.US), 30L));
    when(callbackDelayConfig.getFixedCallbackDelayInSecs())
        .thenReturn(stepRuntimeCallBackDelayConfig);
    StepRuntimeState stepRuntimeState = new StepRuntimeState();
    stepRuntimeState.setStatus(StepInstance.Status.RUNNING);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder().runtimeState(stepRuntimeState).type(StepType.NOOP).build();
    AssertHelper.assertThrows(
        "default config should be defined",
        IllegalArgumentException.class,
        "StepRuntime callback delay config for RUNNING status must contain default step type setting",
        () -> stepRuntimeFixedCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary));
  }
}
