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
import java.util.Set;
import org.junit.Test;

public class RunConfigTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    RunConfig config = loadObject("fixtures/instances/sample-run-config.json", RunConfig.class);
    assertEquals(config, MAPPER.readValue(MAPPER.writeValueAsString(config), RunConfig.class));
  }

  @Test
  public void testStepSelectionFromJson() throws Exception {
    RunConfig config = loadObject("fixtures/instances/sample-run-config.json", RunConfig.class);
    assertEquals(Set.of("load_"), config.getStepSelection().getInclude().getStepIdStartsWith());
    assertEquals(Set.of("load_expensive"), config.getStepSelection().getExclude().getStepIds());
  }
}
