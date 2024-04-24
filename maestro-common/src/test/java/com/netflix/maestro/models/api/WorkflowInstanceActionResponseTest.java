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
package com.netflix.maestro.models.api;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class WorkflowInstanceActionResponseTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerdeForStop() throws Exception {
    WorkflowInstanceActionResponse response =
        loadObject(
            "fixtures/api/sample-workflow-instance-stop-response.json",
            WorkflowInstanceActionResponse.class);
    assertEquals(
        response,
        MAPPER.readValue(
            MAPPER.writeValueAsString(response), WorkflowInstanceActionResponse.class));
  }

  @Test
  public void testRoundTripSerdeForKill() throws Exception {
    WorkflowInstanceActionResponse response =
        loadObject(
            "fixtures/api/sample-workflow-instance-kill-response.json",
            WorkflowInstanceActionResponse.class);
    assertEquals(
        response,
        MAPPER.readValue(
            MAPPER.writeValueAsString(response), WorkflowInstanceActionResponse.class));
  }

  @Test
  public void testRoundTripSerdeForUnblock() throws Exception {
    WorkflowInstanceActionResponse response =
        loadObject(
            "fixtures/api/sample-workflow-instance-unblock-response.json",
            WorkflowInstanceActionResponse.class);
    assertEquals(
        response,
        MAPPER.readValue(
            MAPPER.writeValueAsString(response), WorkflowInstanceActionResponse.class));
  }
}
