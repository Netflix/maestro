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

public class MetadataTest extends MaestroBaseTest {
  public static final String TEST_MANIFEST = "s3://manifest.json";
  public static final String TEST_SOURCE = "s3://source.jar";
  public static final String TEST_SOURCE_DEFINITION = "s3://sourcedef.yaml";
  public static final String TEST_API_VERSION = "3.00";
  public static final String TEST_DSL_CLIENT_VERSION = "3.0.0";
  public static final String TEST_DSL_CLIENT_HOSTNAME = "user-test";
  public static final String TEST_DSL_SOURCE = "nflx-scheduler-client";
  public static final String TEST_DSL_LANG = "java";

  @Test
  public void testRoundTripSerde() throws Exception {
    Metadata md =
        loadObject("fixtures/workflows/definition/sample-valid-metadata.json", Metadata.class);
    assertEquals(md, MAPPER.readValue(MAPPER.writeValueAsString(md), Metadata.class));
  }

  @Test
  public void testFields() throws Exception {
    Metadata md =
        loadObject("fixtures/workflows/definition/sample-valid-metadata.json", Metadata.class);
    assertEquals(TEST_MANIFEST, md.getManifest());
    assertEquals(TEST_SOURCE, md.getSource());
    assertEquals(TEST_SOURCE_DEFINITION, md.getSourceDefinition());
    assertEquals(TEST_API_VERSION, md.getApiVersion());
    assertEquals(TEST_DSL_CLIENT_VERSION, md.getDslClientVersion());
    assertEquals(TEST_DSL_CLIENT_HOSTNAME, md.getDslClientHostName());
    assertEquals(TEST_DSL_SOURCE, md.getDslSource());
    assertEquals(TEST_DSL_LANG, md.getDslLang());
    assertEquals(twoItemMap("step1", 2L, "step2", 1L), md.getStepOrdinals());
  }
}
