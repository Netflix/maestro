package com.netflix.maestro.signal.messageprocessors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.eval.ExprEvaluator;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.properties.SelProperties;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.metrics.MetricConstants;
import com.netflix.maestro.signal.models.SignalTriggerExecution;
import com.netflix.maestro.utils.IdHelper;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for SignalTriggerExecutionProcessor class.
 *
 * @author jun-he
 */
public class SignalTriggerExecutionProcessorTest extends MaestroBaseTest {
  private static ParamEvaluator EVALUATOR;

  private MaestroSignalBrokerDao brokerDao;
  private WorkflowActionHandler actionHandler;
  private MaestroMetricRepo metricRepo;
  private SignalTriggerExecutionProcessor processor;

  @BeforeClass
  public static void init() {
    ExprEvaluator evaluator =
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
            new MaestroParamExtensionRepo(null, null, MAPPER));
    evaluator.postConstruct();
    EVALUATOR = new ParamEvaluator(evaluator, MaestroBaseTest.MAPPER);
  }

  @Before
  public void setup() {
    brokerDao = mock(MaestroSignalBrokerDao.class);
    actionHandler = mock(WorkflowActionHandler.class);
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    processor =
        new SignalTriggerExecutionProcessor(
            brokerDao, EVALUATOR, actionHandler, MAPPER, metricRepo);
  }

  @Test
  public void testProcessStart() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    processor.process(messageSupplier);
    verify(actionHandler, times(1)).start(any(), any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_SUCCESS,
                SignalTriggerExecutionProcessor.class)
            .count());
  }

  @Test
  public void testProcessWithDedupExpr() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setDedupExpr("\"hello\"");
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    processor.process(messageSupplier);
    ArgumentCaptor<RunRequest> captor = ArgumentCaptor.forClass(RunRequest.class);
    verify(actionHandler, times(1)).start(any(), any(), captor.capture());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_SUCCESS,
                SignalTriggerExecutionProcessor.class)
            .count());
    RunRequest request = captor.getValue();
    assertEquals(IdHelper.createUuid("hello"), request.getRequestId());
  }

  @Test
  public void testProcessWithBadDedupExpr() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setDedupExpr("bad expr");
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    processor.process(messageSupplier);
    ArgumentCaptor<RunRequest> captor = ArgumentCaptor.forClass(RunRequest.class);
    verify(actionHandler, times(1)).start(any(), any(), captor.capture());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_SUCCESS,
                SignalTriggerExecutionProcessor.class)
            .count());
    RunRequest request = captor.getValue();
    assertNotEquals(IdHelper.createUuid("hello"), request.getRequestId());
  }

  @Test
  public void testProcessNoStartDueToWrongParams() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setParams(Map.of("foo", StringParamDefinition.builder().expression("abc").build()));
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    processor.process(messageSupplier);
    verify(actionHandler, times(0)).start(any(), any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "param_eval_error")
            .count());
  }

  @Test
  public void testProcessNoStartDueToFalseCondition() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    processor.process(messageSupplier);
    verify(actionHandler, times(0)).start(any(), any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "false_condition")
            .count());
  }

  @Test
  public void testProcessNoStartDueToWrongCondition() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setCondition("abc");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    processor.process(messageSupplier);
    verify(actionHandler, times(0)).start(any(), any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "condition_eval_error")
            .count());
  }

  @Test
  public void testProcessStartWithMaestroResourceConflictException() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    when(actionHandler.start(any(), any(), any()))
        .thenThrow(mock(MaestroResourceConflictException.class));
    processor.process(messageSupplier);
    verify(actionHandler, times(1)).start(any(), any(), any());
    verify(brokerDao, times(1)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "conflict")
            .count());
  }

  @Test
  public void testProcessStartWithWorkflowNotFoundError() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    when(actionHandler.start(any(), any(), any()))
        .thenThrow(new MaestroNotFoundException("has not been created yet"));
    processor.process(messageSupplier);
    verify(actionHandler, times(1)).start(any(), any(), any());
    verify(brokerDao, times(1)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "not_found")
            .count());
  }

  @Test
  public void testProcessStartWithWorkflowNotActiveError() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    when(actionHandler.start(any(), any(), any()))
        .thenThrow(new MaestroNotFoundException("Cannot find an active version for workflow"));
    processor.process(messageSupplier);
    verify(actionHandler, times(1)).start(any(), any(), any());
    verify(brokerDao, times(1)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "inactive")
            .count());
  }

  @Test
  public void testProcessStartWithUnknownNotFoundError() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    when(actionHandler.start(any(), any(), any())).thenThrow(new MaestroNotFoundException("abc"));
    AssertHelper.assertThrows(
        "unknown not found error",
        MaestroRetryableError.class,
        "Unknown 404 not found error for workflow trigger",
        () -> processor.process(messageSupplier));
    verify(actionHandler, times(1)).start(any(), any(), any());
    verify(brokerDao, times(0)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "unknown_not_found")
            .count());
  }

  @Test
  public void testProcessStartWithSignalTriggerDisabledError() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    when(actionHandler.start(any(), any(), any()))
        .thenThrow(
            new MaestroUnprocessableEntityException(
                "Trigger type [SIGNAL] is disabled for the workflow"));
    processor.process(messageSupplier);
    verify(actionHandler, times(1)).start(any(), any(), any());
    verify(brokerDao, times(1)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "trigger_disabled")
            .count());
  }

  @Test
  public void testProcessStartWithUnknownUnprocessableError() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    when(actionHandler.start(any(), any(), any()))
        .thenThrow(new MaestroUnprocessableEntityException("abc"));
    AssertHelper.assertThrows(
        "unknown unprocessable error",
        MaestroRetryableError.class,
        "Unknown unprocessable error for workflow trigger",
        () -> processor.process(messageSupplier));
    verify(actionHandler, times(1)).start(any(), any(), any());
    verify(brokerDao, times(0)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "unknown_unprocessable")
            .count());
  }

  @Test
  public void testProcessStartWithUnknownRuntimeError() throws Exception {
    SignalTriggerExecution execution =
        loadObject("fixtures/sample-signal-trigger-execution.json", SignalTriggerExecution.class);
    execution.setCondition("135 > 2");
    Supplier<SignalTriggerExecution> messageSupplier = () -> execution;
    when(actionHandler.start(any(), any(), any())).thenThrow(new RuntimeException("abc"));
    AssertHelper.assertThrows(
        "unknown runtime error",
        MaestroRetryableError.class,
        "recoverable workflow trigger exception",
        () -> processor.process(messageSupplier));
    verify(actionHandler, times(1)).start(any(), any(), any());
    verify(brokerDao, times(0)).deleteTrigger(any(), any());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
                SignalTriggerExecutionProcessor.class,
                MetricConstants.TYPE_TAG,
                "recoverable")
            .count());
  }
}
