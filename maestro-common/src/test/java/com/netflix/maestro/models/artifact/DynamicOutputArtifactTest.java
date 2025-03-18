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
package com.netflix.maestro.models.artifact;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.maestro.MaestroBaseTest;
import java.util.Map;
import lombok.Data;
import org.junit.Test;

public class DynamicOutputArtifactTest extends MaestroBaseTest {
  @Data
  private static class Artifacts {
    @JsonProperty("artifacts")
    private Map<String, Artifact> artifacts;
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    DynamicOutputArtifact request =
        loadObject(
            "fixtures/artifact/sample-dynamic-output-artifact.json", DynamicOutputArtifact.class);
    assertEquals(
        request, MAPPER.readValue(MAPPER.writeValueAsString(request), DynamicOutputArtifact.class));
  }

  @Test
  public void testDynamicOutputArtifact() throws Exception {
    Artifacts artifactMap = loadObject("fixtures/artifact/sample-artifacts.json", Artifacts.class);
    DynamicOutputArtifact dynamicOutputArtifact =
        artifactMap.getArtifacts().get(Artifact.Type.DYNAMIC_OUTPUT.key()).asDynamicOutput();
    assertEquals(1, dynamicOutputArtifact.getSignalOutputs().size());
    assertEquals(1, dynamicOutputArtifact.getSignalOutputs().size());
    assertEquals(
        "demo_table",
        dynamicOutputArtifact.getSignalOutputs().getFirst().getEvaluatedResult().get("name"));
    assertEquals(
        1536787990L,
        dynamicOutputArtifact.getSignalOutputs().getFirst().getEvaluatedResult().get("timestamp"));
    assertEquals("sample info log", dynamicOutputArtifact.getInfo().getMessage());
  }
}
