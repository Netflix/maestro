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
package com.netflix.maestro.flow.actor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.netflix.maestro.flow.FlowBaseTest;
import com.netflix.maestro.flow.engine.ExecutionContext;
import com.netflix.maestro.flow.models.FlowGroup;
import com.netflix.maestro.flow.properties.FlowEngineProperties;
import com.netflix.maestro.metrics.MaestroMetrics;
import org.junit.Before;
import org.mockito.Mock;

public abstract class ActorBaseTest extends FlowBaseTest {
  @Mock protected ExecutionContext context;
  @Mock protected FlowEngineProperties properties;
  @Mock protected MaestroMetrics metrics;

  @Before
  public void initialize() {
    when(context.getProperties()).thenReturn(properties);
    when(properties.getActorErrorRetryIntervalInMillis()).thenReturn(3000L);
    when(properties.getHeartbeatIntervalInMillis()).thenReturn(10000L);
    when(properties.getGroupFlowFetchLimit()).thenReturn(100L);
    when(properties.getFlowReconciliationIntervalInMillis()).thenReturn(30000L);
    when(context.getMetrics()).thenReturn(metrics);
    when(context.cloneTask(any())).thenAnswer(i -> i.getArguments()[0]);
  }

  GroupActor createGroupActor() {
    FlowGroup group = new FlowGroup(1, 2, "testAddress", 12345);
    return new GroupActor(group, context);
  }

  void verifyEmptyAction(BaseActor actor) {
    assertTrue(actor.getActions().isEmpty());
  }

  void verifyActions(BaseActor actor, Action... expectedActions) {
    var actions = actor.getActions();
    assertEquals(expectedActions.length, actions.size());
    for (Action expectedAction : expectedActions) {
      assertEquals(expectedAction, actions.poll());
    }
  }
}
