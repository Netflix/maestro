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
package com.netflix.maestro.engine.handlers;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.transformation.WorkflowTranslator;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class WorkflowRunnerTest extends MaestroEngineBaseTest {

  @Mock private FlowOperation flowOperation;
  @Mock private WorkflowTranslator translator;
  @Mock private WorkflowHelper workflowHelper;

  private WorkflowRunner runner;

  @Before
  public void before() {
    this.runner = new WorkflowRunner(flowOperation, translator, workflowHelper);
    when(flowOperation.startFlow(anyLong(), any(), any(), any(), anyMap())).thenReturn("test-uuid");
  }

  @Test
  public void testStart() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("test-workflow");
    instance.setMaxGroupNum(5L);
    instance.setWorkflowVersionId(1);
    instance.setRuntimeWorkflow(mock(Workflow.class));
    instance.setRuntimeDag(Collections.singletonMap("step1", new StepTransition()));
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        Collections.singletonMap(
            "stepid",
            Collections.singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));
    instance.setStepRunParams(stepRunParams);
    assertEquals("test-uuid", runner.start(instance));
    verify(translator, times(1)).translate(instance);
    verify(flowOperation, times(1)).startFlow(anyLong(), any(), any(), any(), anyMap());
  }

  @Test
  public void testRestart() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("test-workflow");
    instance.setMaxGroupNum(5L);
    instance.setWorkflowVersionId(1);
    instance.setRuntimeWorkflow(mock(Workflow.class));
    instance.setRuntimeDag(Collections.singletonMap("step1", new StepTransition()));
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        Collections.singletonMap(
            "stepid",
            Collections.singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));
    instance.setStepRunParams(stepRunParams);
    assertEquals("test-uuid", runner.restart(instance));
    verify(translator, times(1)).translate(instance);
    verify(flowOperation, times(1)).startFlow(anyLong(), any(), any(), any(), anyMap());
  }
}
