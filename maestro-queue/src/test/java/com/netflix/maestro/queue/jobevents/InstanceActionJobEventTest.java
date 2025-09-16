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
package com.netflix.maestro.queue.jobevents;

import com.netflix.maestro.MaestroBaseTest;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class InstanceActionJobEventTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    for (String json :
        List.of(
            "fixtures/jobevents/sample-instance-step-action-job-event.json",
            "fixtures/jobevents/sample-instance-workflow-action-job-event.json",
            "fixtures/jobevents/sample-instance-flow-action-job-event.json",
            "fixtures/jobevents/sample-instance-task-action-job-event.json")) {
      InstanceActionJobEvent sampleEvent = loadObject(json, InstanceActionJobEvent.class);
      Assert.assertEquals(
          sampleEvent,
          MaestroBaseTest.MAPPER.readValue(
              MaestroBaseTest.MAPPER.writeValueAsString(sampleEvent),
              InstanceActionJobEvent.class));
    }
  }
}
