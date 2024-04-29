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
package com.netflix.maestro.engine.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.steps.NoOpStepRuntime;
import com.netflix.maestro.engine.steps.SleepStepRuntime;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.definition.WorkflowDefinitionExtras;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class WorkflowEnrichmentHelperTest extends MaestroEngineBaseTest {

  @Mock private ParamsManager paramsManager;
  private WorkflowEnrichmentHelper workflowEnrichmentHelper;
  private WorkflowDefinition definition;

  @Before
  public void setup() throws IOException {
    ImmutableMap<StepType, StepRuntime> stepRuntimeMap =
        ImmutableMap.of(
            StepType.SLEEP, new SleepStepRuntime(), StepType.NOOP, new NoOpStepRuntime());
    workflowEnrichmentHelper = new WorkflowEnrichmentHelper(paramsManager, stepRuntimeMap);
    definition =
        loadObject(
            "fixtures/workflows/definition/sample-minimal-wf.json", WorkflowDefinition.class);
  }

  @Test
  public void testEnrichWorkflowDefinitionParams() {
    Map<String, ParamDefinition> stepParams =
        Collections.singletonMap("sp1", ParamDefinition.buildParamDefinition("sp1", "sv1"));
    Map<String, ParamDefinition> workflowParams =
        Collections.singletonMap("wp1", ParamDefinition.buildParamDefinition("wp1", "wv1"));
    when(paramsManager.generateStaticStepParamDefs(any(), any(), any())).thenReturn(stepParams);
    when(paramsManager.generatedStaticWorkflowParamDefs(any())).thenReturn(workflowParams);
    workflowEnrichmentHelper.enrichWorkflowDefinition(definition);
    Assert.assertNotNull(definition.getEnrichedExtras());
    Assert.assertNull(definition.getEnrichedExtras().getNextExecutionTime());
    WorkflowDefinitionExtras enriched = definition.getEnrichedExtras();
    Assert.assertEquals("wv1", enriched.getWorkflowParams().get("wp1").getValue());
    Assert.assertEquals("sv1", enriched.getStepParams().get("job1").get("sp1").getValue());
  }

  @Test
  public void testEnrichWorkflowDefinitionCron() throws IOException {
    definition =
        loadObject(
            "fixtures/workflows/definition/sample-active-wf-with-cron-named-triggers.json",
            WorkflowDefinition.class);
    workflowEnrichmentHelper.enrichWorkflowDefinition(definition);
    Assert.assertNotNull(definition.getEnrichedExtras());
    Assert.assertNotNull(definition.getEnrichedExtras().getNextExecutionTime());
    Assert.assertEquals(2, definition.getEnrichedExtras().getNextExecutionTimes().size());
  }
}
