/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.flow.models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class MessagePayloadTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    MessagePayload actual =
        MAPPER.readValue(MAPPER.writeValueAsString(MessagePayload.DEFAULT), MessagePayload.class);
    assertTrue(actual instanceof DefaultMessagePayload);
    assertEquals(MessagePayload.Type.DEFAULT, actual.getType());
  }

  @Test
  public void testDeserializeEmptyBodyToDefault() throws Exception {
    MessagePayload actual = MAPPER.readValue("{}", MessagePayload.class);
    assertTrue(actual instanceof DefaultMessagePayload);
    assertEquals(MessagePayload.Type.DEFAULT, actual.getType());
  }
}
