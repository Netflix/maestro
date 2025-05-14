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
package com.netflix.maestro.engine.publisher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.events.MaestroEvent;
import com.netflix.maestro.models.events.WorkflowDefinitionChangeEvent;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sns.SnsClient;

public class SnsEventNotificationPublisherTest extends MaestroBaseTest {
  private SnsClient amazonSns;
  private SnsEventNotificationPublisher client;

  @Before
  public void setup() {
    amazonSns = mock(SnsClient.class);
    client = new SnsEventNotificationPublisher(amazonSns, "sns-topic-test", MAPPER);
  }

  @Test
  public void testSend() {
    MaestroEvent event = WorkflowDefinitionChangeEvent.builder().workflowId("test-wf").build();
    client.send(event);
    verify(amazonSns, Mockito.times(1)).publish(any(Consumer.class));
  }
}
