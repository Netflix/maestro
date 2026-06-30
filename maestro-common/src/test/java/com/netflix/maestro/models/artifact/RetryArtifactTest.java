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
package com.netflix.maestro.models.artifact;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class RetryArtifactTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    RetryArtifact artifact =
        loadObject("fixtures/artifact/sample-retry-artifact.json", RetryArtifact.class);
    assertEquals(
        artifact, MAPPER.readValue(MAPPER.writeValueAsString(artifact), RetryArtifact.class));
  }

  @Test
  public void testDeserializeRetryable() throws Exception {
    Artifact artifact =
        MAPPER.readValue(
            """
            {"type": "RETRY", "retryable": false}
            """, Artifact.class);
    assertEquals(Artifact.Type.RETRY, artifact.getType());
    assertFalse(artifact.asRetry().isRetryable());
  }

  @Test
  public void testDeserializeDefaultsToRetryable() throws Exception {
    Artifact artifact =
        MAPPER.readValue("""
            {"type": "RETRY"}
            """, Artifact.class);
    assertTrue(artifact.asRetry().isRetryable());
  }
}
