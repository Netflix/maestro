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
package com.netflix.maestro.models.signal;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import java.util.Collections;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class SignalOutputsTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    SignalOutputs outputs =
        loadObject("fixtures/signal/sample-signal-outputs.json", SignalOutputs.class);
    assertEquals(
        outputs, MAPPER.readValue(MAPPER.writeValueAsString(outputs), SignalOutputs.class));
  }

  @Test
  public void testSerde() throws Exception {
    var output = new SignalOutputs.SignalOutput();
    output.setName("sigOutput");
    output.setParams(Map.of("p1", SignalParamValue.of(1L), "p2", SignalParamValue.of("v2")));
    output.setSignalId(123L);
    output.setAnnounceTime(123456L);
    SignalOutputs signalOutputs = new SignalOutputs();
    signalOutputs.setOutputs(Collections.singletonList(output));

    Assertions.assertThat(
            MAPPER.readValue(MAPPER.writeValueAsString(signalOutputs), SignalOutputs.class))
        .usingRecursiveComparison()
        .isEqualTo(signalOutputs);
  }
}
