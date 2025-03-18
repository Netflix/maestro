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
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.DatabaseSourceProvider;
import com.netflix.maestro.engine.MaestroDBTestConfiguration;
import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.handlers.SignalHandler;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.properties.SelProperties;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.exceptions.MaestroInvalidExpressionException;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.initiator.SignalInitiator;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalParamValue;
import com.netflix.maestro.models.trigger.SignalTrigger;
import com.netflix.spectator.api.DefaultRegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

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
        null,
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
        null,
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
    extensionRepo.reset(Collections.emptyMap(), null, InstanceWrapper.builder().build());
    assertEquals(
        "test",
        exprEvaluator.eval("return params.getExecutionEnvironment();", Collections.emptyMap()));
  }

  @Test
  public void testGetFromSignalInitiator() {
    SignalInitiator initiator = new SignalInitiator();
    initiator.setParams(
        twoItemMap(
            "param1",
            StringParameter.builder().evaluatedResult("value1").build(),
            "param2",
            LongParameter.builder().evaluatedResult(123L).build()));
    extensionRepo.reset(
        Collections.emptyMap(), null, InstanceWrapper.builder().initiator(initiator).build());

    assertEquals(
        "value1",
        exprEvaluator.eval("return params.getFromSignal('param1');", Collections.emptyMap()));

    assertEquals(
        123L, exprEvaluator.eval("return params.getFromSignal('param2');", Collections.emptyMap()));
    extensionRepo.clear();
  }

  @Test
  public void testGetFromSignal() {
    SignalHandler handler = Mockito.mock(SignalHandler.class);
    SignalInitiator initiator = new SignalInitiator();
    initiator.setSignalIdMap(Map.of("signal-a", 12L, "signal-b", 56L));
    SignalInstance instance1 = new SignalInstance();
    instance1.setParams(Collections.singletonMap("param1", SignalParamValue.of("value1")));
    when(handler.getSignalInstance("signal-a", 12)).thenReturn(instance1);
    SignalInstance instance2 = new SignalInstance();
    instance2.setParams(Collections.singletonMap("param2", SignalParamValue.of(123L)));
    when(handler.getSignalInstance("signal-b", 56)).thenReturn(instance2);
    extensionRepo.reset(
        Collections.emptyMap(), handler, InstanceWrapper.builder().initiator(initiator).build());

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
    SignalHandler handler = Mockito.mock(SignalHandler.class);
    SignalInitiator initiator = new SignalInitiator();
    initiator.setSignalIdMap(Map.of("signal-a", 12L, "signal-b", 56L));
    SignalInstance instance1 = new SignalInstance();
    instance1.setParams(Collections.singletonMap("param1", SignalParamValue.of("value1")));
    when(handler.getSignalInstance("signal-a", 12)).thenReturn(instance1);
    SignalInstance instance2 = new SignalInstance();
    instance2.setParams(Collections.singletonMap("param2", SignalParamValue.of(123L)));
    when(handler.getSignalInstance("signal-b", 56)).thenReturn(instance2);
    extensionRepo.reset(
        Collections.emptyMap(), handler, InstanceWrapper.builder().initiator(initiator).build());

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
    signalTrigger.setDefinitions(
        Collections.singletonMap("signal-name", new SignalTrigger.SignalTriggerEntry()));
    extensionRepo.reset(
        Collections.emptyMap(),
        null,
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
    signalTrigger.setDefinitions(
        Collections.singletonMap("signal-name", new SignalTrigger.SignalTriggerEntry()));
    extensionRepo.reset(
        Collections.emptyMap(),
        null,
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
        null,
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
  public void testGetFromSignalDependency() throws Exception {
    SignalHandler handler = Mockito.mock(SignalHandler.class);
    StepRuntimeSummary runtimeSummary =
        loadObject(
            "fixtures/execution/step-runtime-summary-with-step-dependencies.json",
            StepRuntimeSummary.class);
    extensionRepo.reset(
        Collections.emptyMap(),
        handler,
        InstanceWrapper.builder()
            .stepInstanceAttributes(StepInstanceAttributes.from(runtimeSummary))
            .build());
    SignalInstance instance = new SignalInstance();
    instance.setParams(Collections.singletonMap("param1", SignalParamValue.of("hello")));
    when(handler.getSignalInstance("db/test/table1", 849086)).thenReturn(instance);

    assertEquals(
        "hello",
        exprEvaluator.eval(
            "return params.getFromSignalDependency('0', 'param1');", Collections.emptyMap()));
    extensionRepo.clear();
  }
}
