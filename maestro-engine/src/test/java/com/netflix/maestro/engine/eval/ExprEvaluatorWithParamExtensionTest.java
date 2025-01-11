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
package com.netflix.maestro.engine.eval;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.DatabaseSourceProvider;
import com.netflix.maestro.engine.MaestroDBTestConfiguration;
import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.properties.SelProperties;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.exceptions.MaestroInvalidExpressionException;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.initiator.SignalInitiator;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.StringMapParameter;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.models.trigger.SignalTrigger;
import com.netflix.spectator.api.DefaultRegistry;
import java.io.IOException;
import java.util.Collections;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExprEvaluatorWithParamExtensionTest extends MaestroBaseTest {
  private static final String TEST_WORKFLOW_ID = "maestro_foreach_inline-123";
  private static final String TEST_STEP_INSTANCE =
      "fixtures/instances/sample-inline-workflow-step-instance.json";
  private static final String TEST_STEP_RUNTIME_SUMMARY =
      "fixtures/execution/sample-step-runtime-summary-2.json";

  private static DataSource dataSource;
  private static MaestroStepInstanceDao stepDao;
  private static MaestroParamExtensionRepo extensionRepo;
  private static ExprEvaluator exprEvaluator;

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
    DatabaseConfiguration config = new MaestroDBTestConfiguration();
    dataSource = new DatabaseSourceProvider(config).get();
    stepDao =
        new MaestroStepInstanceDao(
            dataSource, MAPPER, config, new MaestroMetricRepo(new DefaultRegistry()));
    extensionRepo = new MaestroParamExtensionRepo(stepDao, "test", MAPPER);
    exprEvaluator =
        new ExprEvaluator(
            SelProperties.builder()
                .threadNum(3)
                .timeoutMillis(120000)
                .stackLimit(128)
                .loopLimit(10000)
                .arrayLimit(10000)
                .lengthLimit(10000)
                .visitLimit(100000000L)
                .memoryLimit(10000000L)
                .build(),
            extensionRepo);
    exprEvaluator.postConstruct();
  }

  @AfterClass
  public static void destroy() {
    exprEvaluator.preDestroy();
    MaestroBaseTest.destroy();
  }

  @Before
  public void setUp() throws Exception {
    StepInstance si = loadObject(TEST_STEP_INSTANCE, StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, false);
  }

  @After
  public void tearDown() {
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 1);
    AssertHelper.assertThrows(
        "cannot get non-existing workflow instance",
        MaestroNotFoundException.class,
        "workflow instance [maestro_foreach_inline-123][1][1]'s step instance [job1][1] not found",
        () -> stepDao.getStepInstance(TEST_WORKFLOW_ID, 1, 1, "job1", "1"));
  }

  @Test
  public void testGetFromStep() throws Exception {
    StepRuntimeSummary summary = loadObject(TEST_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    extensionRepo.reset(
        Collections.singletonMap(
            "foreach-job", Collections.singletonMap("maestro_step_runtime_summary", summary)),
        Collections.emptyMap(),
        InstanceWrapper.builder().build());

    assertEquals(
        "foo",
        exprEvaluator.eval(
            "return params.getFromStep('foreach-job', 'param1');", Collections.emptyMap()));
    extensionRepo.clear();
  }

  @Test
  public void testGetFromCurrentStep() throws IOException {
    StepRuntimeSummary summary = loadObject(TEST_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    extensionRepo.reset(
        Collections.singletonMap(
            "foreach-job", Collections.singletonMap("maestro_step_runtime_summary", summary)),
        Collections.emptyMap(),
        InstanceWrapper.builder()
            .stepInstanceAttributes(StepInstanceAttributes.from(summary))
            .build());
    assertEquals(
        "foreach-job",
        exprEvaluator.eval("return params.getFromStep('step_id');", Collections.emptyMap()));
    assertEquals(
        123L,
        exprEvaluator.eval(
            "return params.getFromStep('step_instance_id');", Collections.emptyMap()));
    assertEquals(
        "bar",
        exprEvaluator.eval(
            "return params.getFromStep('step_instance_uuid');", Collections.emptyMap()));
    assertEquals(
        "FOREACH",
        exprEvaluator.eval("return params.getFromStep('step_type_info');", Collections.emptyMap()));
  }

  @Test
  public void testGetExecutionEnvironment() {
    extensionRepo.reset(
        Collections.emptyMap(), Collections.emptyMap(), InstanceWrapper.builder().build());
    assertEquals(
        "test",
        exprEvaluator.eval("return params.getExecutionEnvironment();", Collections.emptyMap()));
  }

  @Test
  public void testGetFromSignal() {
    SignalInitiator initiator = new SignalInitiator();
    initiator.setParams(
        twoItemMap(
            "signal-a",
            StringMapParameter.builder().evaluatedResult(singletonMap("param1", "value1")).build(),
            "signal-b",
            MapParameter.builder().evaluatedResult(singletonMap("param2", 123L)).build()));
    extensionRepo.reset(
        Collections.emptyMap(),
        Collections.emptyMap(),
        InstanceWrapper.builder().initiator(initiator).build());

    assertEquals(
        "value1",
        exprEvaluator.eval(
            "return params.getFromSignal('signal-a', 'param1');", Collections.emptyMap()));

    assertEquals(
        123L,
        exprEvaluator.eval(
            "return params.getFromSignal('signal-b', 'param2');", Collections.emptyMap()));
    extensionRepo.clear();
  }

  @Test
  public void testGetFromSignalOrDefault() {
    SignalInitiator initiator = new SignalInitiator();
    initiator.setParams(
        twoItemMap(
            "signal-a",
            StringMapParameter.builder().evaluatedResult(singletonMap("param1", "value1")).build(),
            "signal-b",
            MapParameter.builder().evaluatedResult(singletonMap("param2", 123L)).build()));
    extensionRepo.reset(
        Collections.emptyMap(),
        Collections.emptyMap(),
        InstanceWrapper.builder().initiator(initiator).build());

    assertEquals(
        "value1",
        exprEvaluator.eval(
            "return params.getFromSignalOrDefault('signal-a', 'param1', 'defaultValue');",
            Collections.emptyMap()));

    assertEquals(
        "defaultValue",
        exprEvaluator.eval(
            "return params.getFromSignalOrDefault('signal-a', 'nonExistingParam', 'defaultValue');",
            Collections.emptyMap()));

    assertEquals(
        "defaultValue",
        exprEvaluator.eval(
            "return params.getFromSignalOrDefault('signal-non-existing', 'nonExistingParam', 'defaultValue');",
            Collections.emptyMap()));
    extensionRepo.clear();
  }

  @Test
  public void testGetFromSignalForDryRunValidator() {
    SignalTrigger signalTrigger = new SignalTrigger();
    signalTrigger.setDefinition(Collections.singletonMap("signal-name", Collections.emptyMap()));
    extensionRepo.reset(
        Collections.emptyMap(),
        Collections.emptyMap(),
        InstanceWrapper.builder()
            .initiator(new DryRunValidator.ValidationInitiator())
            .signalTriggers(Collections.singletonList(signalTrigger))
            .build());

    Assert.assertEquals(
        MaestroParamExtension.DUMMY_VALIDATION_VALUE,
        exprEvaluator.eval(
            "return params.getFromSignal('signal-name', 'param-name');", Collections.emptyMap()));
    extensionRepo.clear();
  }

  @Test
  public void testGetFromSignalForDryRunValidatorWithInvalidSignal() {
    SignalTrigger signalTrigger = new SignalTrigger();
    signalTrigger.setDefinition(Collections.singletonMap("signal-name", Collections.emptyMap()));
    extensionRepo.reset(
        Collections.emptyMap(),
        Collections.emptyMap(),
        InstanceWrapper.builder()
            .initiator(new DryRunValidator.ValidationInitiator())
            .signalTriggers(Collections.singletonList(signalTrigger))
            .build());

    AssertHelper.assertThrows(
        "cannot get non-existing signal",
        MaestroInvalidExpressionException.class,
        "Expression evaluation throws an exception",
        () ->
            exprEvaluator.eval(
                "return params.getFromSignal('signal-not-existing', 'param-name');",
                Collections.emptyMap()));
    extensionRepo.clear();
  }

  @Test
  public void testGetFromForeach() throws Exception {
    StepRuntimeSummary summary = loadObject(TEST_STEP_RUNTIME_SUMMARY, StepRuntimeSummary.class);
    extensionRepo.reset(
        Collections.singletonMap(
            "foreach-job", Collections.singletonMap("maestro_step_runtime_summary", summary)),
        Collections.emptyMap(),
        InstanceWrapper.builder().build());
    long[] res =
        (long[])
            exprEvaluator.eval(
                "return params.getFromForeach('foreach-job', 'job1', 'sleep_seconds');",
                Collections.emptyMap());
    assertArrayEquals(new long[] {15, 0, 0, 0, 0, 0}, res);
    extensionRepo.clear();
  }

  @Test
  public void testGetFromSignalDependency() {
    extensionRepo.reset(
        Collections.emptyMap(),
        Collections.singletonMap(
            "dev/foo/bar",
            Collections.singletonList(
                Collections.singletonMap(
                    "param1", StringParameter.builder().evaluatedResult("hello").build()))),
        InstanceWrapper.builder().build());

    assertEquals(
        "hello",
        exprEvaluator.eval(
            "return params.getFromSignalDependency('dev/foo/bar', 'param1');",
            Collections.emptyMap()));
    extensionRepo.clear();
  }
}
