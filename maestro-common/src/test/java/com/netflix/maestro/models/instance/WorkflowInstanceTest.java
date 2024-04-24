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
package com.netflix.maestro.models.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.netflix.maestro.MaestroBaseTest;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkflowInstanceTest extends MaestroBaseTest {

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    for (String fileName :
        Arrays.asList(
            "sample-workflow-instance-created.json", "sample-workflow-instance-succeeded.json")) {
      WorkflowInstance expected =
          loadObject("fixtures/instances/" + fileName, WorkflowInstance.class);
      String ser1 = MAPPER.writeValueAsString(expected);
      WorkflowInstance actual =
          MAPPER.readValue(MAPPER.writeValueAsString(expected), WorkflowInstance.class);
      String ser2 = MAPPER.writeValueAsString(actual);
      assertEquals(expected, actual);
      assertEquals(ser1, ser2);
      assertFalse(actual.isInlineWorkflow());
    }
  }

  @Test
  public void testRunStatus() throws Exception {
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-succeeded.json", WorkflowInstance.class);
    assertEquals(WorkflowInstance.Status.SUCCEEDED, instance.getStatus());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, instance.getRunStatus());
    instance.setStatus(WorkflowInstance.Status.FAILED);
    instance.getRuntimeOverview().setRunStatus(WorkflowInstance.Status.SUCCEEDED);
    assertEquals(WorkflowInstance.Status.FAILED, instance.getStatus());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, instance.getRunStatus());
    assertEquals(12345, instance.getInternalId().longValue());
  }
}
