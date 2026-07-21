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
package com.netflix.maestro.engine.params;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamType;
import java.io.IOException;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DefaultParamManagerTest extends MaestroEngineBaseTest {

  private DefaultParamManager defaultParamManager;

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  private void verifyNonNullMap(Map<String, ParamDefinition> defaultWorkflowParams) {
    defaultWorkflowParams.forEach(
        (k, v) -> {
          if (v.getType() == ParamType.MAP) {
            assertNotNull(
                String.format("Default MAP param value for [%s] cannot be null", k), v.getValue());
          }
        });
  }

  @Before
  public void setUp() throws IOException {
    defaultParamManager = new DefaultParamManager(YAML_MAPPER);
    defaultParamManager.init();
  }

  @Test
  public void testValidDefaultWorkflowParams() {
    assertFalse(defaultParamManager.getDefaultWorkflowParams().isEmpty());
    assertNotNull(defaultParamManager.getDefaultWorkflowParams().get("TARGET_RUN_HOUR").getName());
  }

  @Test
  public void testValidDefaultDryRunParams() {
    assertFalse(defaultParamManager.getDefaultDryRunParams().isEmpty());
    assertNotNull(defaultParamManager.getDefaultDryRunParams().get("FROM_DATE"));
  }

  @Test
  public void testDefaultWorkflowParamsMutate() {
    defaultParamManager
        .getDefaultWorkflowParams()
        .put("TEST", ParamDefinition.buildParamDefinition("TEST", "123"));
    assertNull(defaultParamManager.getDefaultWorkflowParams().get("TEST"));
  }

  @Test
  public void testValidDefaultStepParams() {
    assertFalse(defaultParamManager.getDefaultStepParams().isEmpty());
    assertNotNull(defaultParamManager.getDefaultStepParams().get("workflow_instance_id").getName());
  }

  @Test
  public void testDefaultStepsParamsMutate() {
    defaultParamManager
        .getDefaultStepParams()
        .put("TEST", ParamDefinition.buildParamDefinition("TEST", "123"));
    assertNull(defaultParamManager.getDefaultStepParams().get("TEST"));
  }

  @Test
  public void testExistsStepTypeParams() {
    assertTrue(defaultParamManager.getDefaultParamsForType(StepType.SUBWORKFLOW).isPresent());
    assertTrue(defaultParamManager.getDefaultParamsForType(StepType.FOREACH).isPresent());
  }

  @Test
  public void testNonNullMapTypes() {
    verifyNonNullMap(defaultParamManager.getDefaultWorkflowParams());
    verifyNonNullMap(defaultParamManager.getDefaultStepParams());
    verifyNonNullMap(defaultParamManager.getDefaultParamsForType(StepType.SUBWORKFLOW).get());
    verifyNonNullMap(defaultParamManager.getDefaultParamsForType(StepType.FOREACH).get());
  }

  @Test
  public void testNullStepTypeParams() {
    assertFalse(defaultParamManager.getDefaultParamsForType(StepType.TITUS).isPresent());
  }

  @Test
  public void testStepTypeParamsMutate() {
    defaultParamManager
        .getDefaultParamsForType(StepType.FOREACH)
        .get()
        .put("TEST", ParamDefinition.buildParamDefinition("TEST", "123"));
    assertNull(defaultParamManager.getDefaultParamsForType(StepType.FOREACH).get().get("TEST"));
  }

  @Test
  public void testWorkflowParamOverrideMergesAndAdds() throws IOException {
    Map<String, String> overrides =
        Map.of(
            DefaultParamManager.WORKFLOW_OVERRIDE_KEY,
            "TARGET_RUN_HOUR:\n"
                + "  value: OVERRIDDEN\n"
                + "  type: STRING\n"
                + "MY_ORG_PARAM:\n"
                + "  value: hello\n"
                + "  type: STRING\n");
    DefaultParamManager manager = new DefaultParamManager(YAML_MAPPER, overrides);
    manager.init();

    Map<String, ParamDefinition> params = manager.getDefaultWorkflowParams();
    assertNotNull(params.get("TARGET_RUN_DATE"));
    assertEquals("OVERRIDDEN", params.get("TARGET_RUN_HOUR").getValue());
    assertEquals("hello", params.get("MY_ORG_PARAM").getValue());
  }

  @Test
  public void testByTypeOverrideAddsParamsForTypeWithoutBundledFile() throws IOException {
    Map<String, String> overrides =
        Map.of("titus", "my_titus_param:\n  value: v\n  type: STRING\n");
    DefaultParamManager manager = new DefaultParamManager(YAML_MAPPER, overrides);
    manager.init();

    assertTrue(manager.getDefaultParamsForType(StepType.TITUS).isPresent());
    assertEquals(
        "v",
        manager.getDefaultParamsForType(StepType.TITUS).get().get("my_titus_param").getValue());
  }

  @Test
  public void testNoOverrideLeavesDefaultsUnchanged() throws IOException {
    DefaultParamManager manager = new DefaultParamManager(YAML_MAPPER, null);
    manager.init();

    assertNotNull(manager.getDefaultWorkflowParams().get("TARGET_RUN_HOUR").getName());
    assertFalse(manager.getDefaultParamsForType(StepType.TITUS).isPresent());
  }
}
