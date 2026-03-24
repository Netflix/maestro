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
package com.netflix.maestro.extensions.utils;

import static junit.framework.TestCase.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.instance.StepInstance.Status;
import org.junit.Test;

public class StepInstanceStatusEncoderTest extends MaestroBaseTest {
  @Test
  public void allStepInstanceStatusesShouldBeEncodedAndNotThrow() {
    for (Status status : Status.values()) {
      StepInstanceStatusEncoder.encode(status);
    }
  }

  @Test
  public void stepInstanceStatusShouldBeEncodedToRightValue() {
    assertEquals(0L, StepInstanceStatusEncoder.encode(Status.NOT_CREATED));
    assertEquals(10L, StepInstanceStatusEncoder.encode(Status.CREATED));
    assertEquals(20L, StepInstanceStatusEncoder.encode(Status.INITIALIZED));
    assertEquals(30L, StepInstanceStatusEncoder.encode(Status.PAUSED));
    assertEquals(40L, StepInstanceStatusEncoder.encode(Status.WAITING_FOR_SIGNALS));
    assertEquals(50L, StepInstanceStatusEncoder.encode(Status.EVALUATING_PARAMS));
    assertEquals(60L, StepInstanceStatusEncoder.encode(Status.WAITING_FOR_PERMITS));
    assertEquals(70L, StepInstanceStatusEncoder.encode(Status.STARTING));
    assertEquals(80L, StepInstanceStatusEncoder.encode(Status.RUNNING));
    assertEquals(90L, StepInstanceStatusEncoder.encode(Status.FINISHING));
    assertEquals(100L, StepInstanceStatusEncoder.encode(Status.DISABLED));
    assertEquals(101L, StepInstanceStatusEncoder.encode(Status.UNSATISFIED));
    assertEquals(102L, StepInstanceStatusEncoder.encode(Status.SKIPPED));
    assertEquals(103L, StepInstanceStatusEncoder.encode(Status.SUCCEEDED));
    assertEquals(104L, StepInstanceStatusEncoder.encode(Status.COMPLETED_WITH_ERROR));
    assertEquals(105L, StepInstanceStatusEncoder.encode(Status.USER_FAILED));
    assertEquals(106L, StepInstanceStatusEncoder.encode(Status.PLATFORM_FAILED));
    assertEquals(107L, StepInstanceStatusEncoder.encode(Status.FATALLY_FAILED));
    assertEquals(108L, StepInstanceStatusEncoder.encode(Status.INTERNALLY_FAILED));
    assertEquals(109L, StepInstanceStatusEncoder.encode(Status.STOPPED));
    assertEquals(110L, StepInstanceStatusEncoder.encode(Status.TIMED_OUT));
    assertEquals(111L, StepInstanceStatusEncoder.encode(Status.TIMEOUT_FAILED));
  }
}
