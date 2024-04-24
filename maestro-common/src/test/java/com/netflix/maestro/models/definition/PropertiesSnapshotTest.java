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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class PropertiesSnapshotTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    PropertiesSnapshot ps =
        loadObject(
            "fixtures/workflows/definition/sample-properties.json", PropertiesSnapshot.class);
    assertEquals(ps, MAPPER.readValue(MAPPER.writeValueAsString(ps), PropertiesSnapshot.class));
  }

  @Test
  public void testExtractProperties() throws Exception {
    PropertiesSnapshot ps =
        loadObject(
            "fixtures/workflows/definition/sample-properties.json", PropertiesSnapshot.class);
    Properties extracted = ps.extractProperties();
    assertEquals(ps.getOwner(), extracted.getOwner());
    assertEquals(ps.getAccessControl(), extracted.getAccessControl());
    assertEquals(ps.getRunStrategy(), extracted.getRunStrategy());
    assertEquals(ps.getStepConcurrency(), extracted.getStepConcurrency());
    assertEquals(ps.getAlerting(), extracted.getAlerting());
    assertEquals(ps.getAlertingDisabled(), extracted.getAlertingDisabled());
    assertEquals(ps.getSignalTriggerDisabled(), extracted.getSignalTriggerDisabled());
    assertEquals(ps.getTimeTriggerDisabled(), extracted.getTimeTriggerDisabled());
    assertEquals(ps.getDescription(), extracted.getDescription());
    assertEquals(ps.getTags(), extracted.getTags());
  }
}
