package com.netflix.maestro.models.artifact;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.MaestroBaseTest;
import lombok.Data;
import org.junit.Test;

/** Covers the contract of {@link Artifact.Type#EXTENSION} for a deployment's own artifact class. */
public class ExtensionArtifactTypeTest extends MaestroBaseTest {
  private static final String JSON =
      """
      {"type": "SPARK", "application_id": "app-1"}
      """;

  /** An artifact a deployment's extension step runtime writes, with its own JSON type name. */
  @Data
  @JsonTypeName("SPARK")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  private static class SparkArtifact implements Artifact {
    private String applicationId;

    @JsonProperty("type")
    public String getTypeName() {
      return "SPARK";
    }

    @JsonIgnore
    @Override
    public Type getType() {
      return Type.EXTENSION;
    }
  }

  @Test
  public void testExtensionTypeHasNoFixedKey() {
    assertNull(Artifact.Type.EXTENSION.key());
    assertEquals(Artifact.Type.EXTENSION, new SparkArtifact().getType());
  }

  @Test
  public void testRegisteredReaderGetsTheDeploymentClass() throws Exception {
    ObjectMapper mapper = MAPPER.copy();
    mapper.registerSubtypes(SparkArtifact.class);
    Artifact artifact = mapper.readValue(JSON, Artifact.class);
    assertTrue(artifact instanceof SparkArtifact);
    assertEquals(Artifact.Type.EXTENSION, artifact.getType());
    assertEquals("app-1", ((SparkArtifact) artifact).getApplicationId());
    assertEquals(artifact, mapper.readValue(mapper.writeValueAsString(artifact), Artifact.class));
  }

  /**
   * A reader without the deployment class falls back to a DEFAULT artifact holding the fields. The
   * type name is not retained, so such a reader identifies the artifact by its map key.
   */
  @Test
  public void testUnregisteredReaderGetsDefaultArtifactWithTheFields() throws Exception {
    Artifact artifact = MAPPER.readValue(JSON, Artifact.class);
    assertEquals(Artifact.Type.DEFAULT, artifact.getType());
    assertEquals("app-1", artifact.asDefault().getField("application_id"));
    assertNull(artifact.asDefault().getField("type"));
  }
}
