package com.netflix.maestro.models.artifact;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
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
    assertEquals(1, dynamicOutputArtifact.getOutputs().size());
    assertEquals(
        1,
        dynamicOutputArtifact.getOutputs().get(StepOutputsDefinition.StepOutputType.SIGNAL).size());
    assertEquals(
        "demo_table",
        dynamicOutputArtifact.getOutputSignals().getFirst().getEvaluatedResult().get("name"));
    assertEquals(
        1536787990L,
        dynamicOutputArtifact.getOutputSignals().getFirst().getEvaluatedResult().get("timestamp"));
    assertEquals("sample info log", dynamicOutputArtifact.getInfo().getMessage());
  }
}
