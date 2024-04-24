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
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.Defaults;
import org.junit.Test;

public class RestartConfigTest extends MaestroBaseTest {

  private void roundTripSerde(String fileName) throws Exception {
    RestartConfig config = loadObject(fileName, RestartConfig.class);
    assertEquals(config, MAPPER.readValue(MAPPER.writeValueAsString(config), RestartConfig.class));
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    roundTripSerde("fixtures/instances/sample-restart-config.json");
    roundTripSerde("fixtures/instances/sample-restart-config-upstream-mode.json");
    roundTripSerde("fixtures/instances/sample-restart-config-skip-steps.json");
  }

  @Test
  public void testBuilderWithDefault() {
    RestartConfig config = RestartConfig.builder().build();
    assertTrue(config.getRestartPath().isEmpty());
    assertEquals(Defaults.DEFAULT_RESTART_POLICY, config.getRestartPolicy());
    assertEquals(Defaults.DEFAULT_RESTART_POLICY, config.getDownstreamPolicy());
  }

  @Test
  public void testAddRestartNode() {
    RestartConfig config =
        RestartConfig.builder()
            .addRestartNode("foo", 1, "bar")
            .addRestartNode("foo", 2, "bar")
            .build();
    assertEquals(2, config.getRestartPath().size());
    assertEquals(Defaults.DEFAULT_RESTART_POLICY, config.getRestartPolicy());
    assertEquals(Defaults.DEFAULT_RESTART_POLICY, config.getDownstreamPolicy());
    assertEquals(1, config.getRestartPath().get(0).getInstanceId());
    assertEquals(2, config.getRestartPath().get(1).getInstanceId());
  }
}
