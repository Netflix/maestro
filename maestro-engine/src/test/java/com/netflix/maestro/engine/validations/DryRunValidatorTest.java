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
package com.netflix.maestro.engine.validations;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.params.DefaultParamManager;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.steps.NoOpStepRuntime;
import com.netflix.maestro.engine.steps.SleepStepRuntime;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.transformation.DagTranslator;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.exceptions.MaestroDryRunException;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class DryRunValidatorTest extends MaestroEngineBaseTest {
  @Mock private DefaultParamManager defaultParamsManager;
  @Mock private ParamsManager paramsManager;
  @Mock private DagTranslator dagTranslator;
  @Mock private MaestroParamExtensionRepo extensionRepo;

  private DryRunValidator dryRunValidator;
  private WorkflowDefinition definition;
  private User user;

  @Before
  public void setup() throws IOException {
    definition =
        loadObject(
            "fixtures/workflows/definition/sample-active-wf-with-props.json",
            WorkflowDefinition.class);
    user = User.create("demo");
    Map<StepType, StepRuntime> stepRuntimeMap = new HashMap<>();
    stepRuntimeMap.put(StepType.SLEEP, new SleepStepRuntime());
    stepRuntimeMap.put(StepType.NOOP, new NoOpStepRuntime());
    WorkflowHelper workflowHelper =
        new WorkflowHelper(paramsManager, paramEvaluator, dagTranslator, extensionRepo, 1);
    dryRunValidator =
        new DryRunValidator(stepRuntimeMap, defaultParamsManager, paramsManager, workflowHelper);
  }

  @Test
  public void testValidatePass() {
    when(paramsManager.generateMergedWorkflowParams(any(), any()))
        .thenReturn(new LinkedHashMap<>());
    when(paramsManager.generateMergedStepParams(any(), any(), any(), any()))
        .thenReturn(new LinkedHashMap<>());
    dryRunValidator.validate(definition.getWorkflow(), user);
  }

  @Test
  public void testValidateFailWorkflowMerge() {
    when(paramsManager.generateMergedWorkflowParams(any(), any()))
        .thenThrow(new MaestroValidationException("Error validating"));
    when(paramsManager.generateMergedStepParams(any(), any(), any(), any()))
        .thenReturn(new LinkedHashMap<>());
    AssertHelper.assertThrows(
        "validation error",
        MaestroDryRunException.class,
        "Exception during dry run validation",
        () -> dryRunValidator.validate(definition.getWorkflow(), user));
  }

  @Test
  public void testValidateFailStepMerge() {
    when(paramsManager.generateMergedWorkflowParams(any(), any()))
        .thenReturn(new LinkedHashMap<>());
    when(paramsManager.generateMergedStepParams(any(), any(), any(), any()))
        .thenThrow(new MaestroValidationException("Error validating"));
    AssertHelper.assertThrows(
        "validation error",
        MaestroDryRunException.class,
        "Exception during dry run validation",
        () -> dryRunValidator.validate(definition.getWorkflow(), user));
  }

  @Test
  public void testValidateDefaultRunParams() {
    definition
        .getWorkflow()
        .getParams()
        .put("FROM_DATE", ParamDefinition.buildParamDefinition("FROM_DATE", "YYYYMMDD"));
    definition
        .getWorkflow()
        .getParams()
        .put("DIFFERENT_TYPE", ParamDefinition.buildParamDefinition("DIFFERENT_TYPE", "YYYYMMDD"));

    Map<String, ParamDefinition> paramDefs = new HashMap<>();
    paramDefs.put("FROM_DATE", ParamDefinition.buildParamDefinition("FROM_DATE", "20210101"));
    paramDefs.put("DIFFERENT_TYPE", ParamDefinition.buildParamDefinition("DIFFERENT_TYPE", 1234));
    paramDefs.put("MISSING", ParamDefinition.buildParamDefinition("MISSING", 1234));
    when(defaultParamsManager.getDefaultDryRunParams()).thenReturn(paramDefs);
    ArgumentCaptor<RunRequest> captor = ArgumentCaptor.forClass(RunRequest.class);
    when(paramsManager.generateMergedWorkflowParams(any(), any()))
        .thenReturn(new LinkedHashMap<>());
    when(paramsManager.generateMergedStepParams(any(), any(), any(), any()))
        .thenReturn(new LinkedHashMap<>());
    dryRunValidator.validate(definition.getWorkflow(), user);

    // should only use default params matching workflow and also matching type of workflow param
    Map<String, ParamDefinition> expectedRunParams =
        Collections.singletonMap(
            "FROM_DATE", ParamDefinition.buildParamDefinition("FROM_DATE", "20210101"));
    verify(paramsManager, times(1)).generateMergedWorkflowParams(any(), captor.capture());
    assertEquals(expectedRunParams, captor.getValue().getRunParams());
  }

  @Test
  public void testExceptionAndTraceForNullMessage() {
    when(paramsManager.generateMergedWorkflowParams(any(), any()))
        .thenThrow(new NumberFormatException());
    when(paramsManager.generateMergedStepParams(any(), any(), any(), any()))
        .thenReturn(new LinkedHashMap<>());
    AssertHelper.assertThrows(
        "validation error",
        MaestroDryRunException.class,
        "Type=[class java.lang.NumberFormatException] StackTrace=[java.lang.NumberFormatException",
        () -> dryRunValidator.validate(definition.getWorkflow(), user));
  }
}
