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
package com.netflix.maestro.flow;

import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowDef;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.mockito.MockitoAnnotations;

public abstract class FlowBaseTest {

  private AutoCloseable closeable;

  @Before
  public void openMocks() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @After
  public void releaseMocks() throws Exception {
    closeable.close();
  }

  protected Flow createFlow() {
    Flow flow =
        new Flow(10, "test-flow-id", 1, System.currentTimeMillis() + 3600000, "test-flow-ref");
    flow.setInput(Map.of());
    flow.setFlowDef(new FlowDef());
    flow.setStatus(Flow.Status.RUNNING);
    flow.setUpdateTime(flow.getStartTime());
    return flow;
  }
}
