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
package com.netflix.maestro.models.artifact;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class TemplateArtifactTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    TemplateArtifact request =
        loadObject("fixtures/artifact/sample-template-artifact.json", TemplateArtifact.class);
    assertEquals(
        request, MAPPER.readValue(MAPPER.writeValueAsString(request), TemplateArtifact.class));
  }

  @Test
  public void testDeserialize() throws Exception {
    Artifact artifact =
        loadObject("fixtures/artifact/sample-template-artifact.json", Artifact.class);
    assertEquals(Artifact.Type.TEMPLATE, artifact.getType());
    TemplateArtifact template = artifact.asTemplate();
    assertEquals(
        "maestro_template_E7W_2G7_8375dda31ebbddc3be29e95e72ea9bc9",
        template.getTemplateWorkflowId());
    assertEquals(1L, template.getTemplateInstanceId());
    assertEquals(2L, template.getTemplateRunId());
    assertEquals("foo-bar", template.getTemplateUuid());
    assertEquals("write_audit_publish", template.getJobType());
    assertEquals("v3", template.getTemplateVersion());
    assertEquals(3L, template.getTemplateOverview().getTotalStepCount());
    assertEquals(
        "[maestro_template_E7W_2G7_8375dda31ebbddc3be29e95e72ea9bc9][1][2][foo-bar]",
        template.getIdentity());
  }
}
