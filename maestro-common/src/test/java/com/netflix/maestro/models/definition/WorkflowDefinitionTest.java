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

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.netflix.maestro.MaestroBaseTest;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class WorkflowDefinitionTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    WorkflowDefinition wfd =
        loadObject(
            "fixtures/workflows/definition/sample-active-wf-with-props.json",
            WorkflowDefinition.class);
    assertEquals(wfd, MAPPER.readValue(MAPPER.writeValueAsString(wfd), WorkflowDefinition.class));
  }

  @Test
  public void testRoundTripSerdeOutputSignals() throws Exception {
    WorkflowDefinition wfd =
        loadObject(
            "fixtures/workflows/definition/sample-active-wf-with-output-signals.json",
            WorkflowDefinition.class);
    Assertions.assertThat(wfd)
        .usingRecursiveComparison()
        .isEqualTo(MAPPER.readValue(MAPPER.writeValueAsString(wfd), WorkflowDefinition.class));
  }

  @Test
  public void testRoundTripSerdeEnriched() throws Exception {
    WorkflowDefinition wfd =
        loadObject(
            "fixtures/workflows/definition/sample-active-wf-with-enriched-extras.json",
            WorkflowDefinition.class);
    assertEquals(wfd, MAPPER.readValue(MAPPER.writeValueAsString(wfd), WorkflowDefinition.class));
  }

  @Test(expected = UnrecognizedPropertyException.class)
  public void testInvalidWorkflowDefinition() throws Exception {
    loadObject("fixtures/workflows/definition/sample-invalid-wf.json", WorkflowDefinition.class);
  }

  @Test
  public void testSubworkflowWithParams() throws Exception {
    WorkflowDefinition wfd =
        loadObject(
            "fixtures/workflows/definition/sample-subworkflow-wf-with-params.json",
            WorkflowDefinition.class);
    assertEquals(wfd, MAPPER.readValue(MAPPER.writeValueAsString(wfd), WorkflowDefinition.class));
  }
}
