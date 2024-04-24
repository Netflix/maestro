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

import com.netflix.maestro.MaestroBaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkflowInstanceAggregatedInfoTest extends MaestroBaseTest {
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    WorkflowInstanceAggregatedInfo expected =
        loadObject(
            "fixtures/aggregated/workflow-aggregated-info.json",
            WorkflowInstanceAggregatedInfo.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    WorkflowInstanceAggregatedInfo actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), WorkflowInstanceAggregatedInfo.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }
}
