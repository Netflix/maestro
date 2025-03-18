package com.netflix.maestro.signal.messageprocessors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.utils.ExceptionClassifier;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.initiator.SignalInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.parameter.BooleanParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.metrics.MetricConstants;
import com.netflix.maestro.signal.models.SignalTriggerExecution;
import com.netflix.maestro.utils.IdHelper;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * It processes {@link SignalTriggerExecution} message. This message processor is responsible to do
 * the postprocessing, such as evaluating params and conditions. If condition is met, it will start
 * the workflow instance. Note that the signal checkpoints have already been moved forward (i.e.
 * signal instances are consumed) no matter whether the condition is met or not. It also does the
 * cleanup based on the response.
 *
 * @author jun-he
 */
@Slf4j
@AllArgsConstructor
public class SignalTriggerExecutionProcessor {
  private final MaestroSignalBrokerDao brokerDao;
  private final ParamEvaluator paramEvaluator;
  private final WorkflowActionHandler actionHandler;
  private final ObjectMapper objectMapper;
  private final MaestroMetrics metrics;

  /**
   * Process the signal trigger execution. It also does the post condition and params evaluation. It
   * should be based on a FIFO queue partitioned by (workflow_id, trigger_uuid) to reduce race
   * condition.
   *
   * @param messageSupplier message supplier
   */
  public void process(Supplier<SignalTriggerExecution> messageSupplier) {
    SignalTriggerExecution execution = messageSupplier.get();
    String workflowId = execution.getWorkflowId();
    String triggerUuid = execution.getTriggerUuid();
    Map<String, Parameter> evaluatedParams = new LinkedHashMap<>();
    if (execution.getParams() != null) {
      for (var entry : execution.getParams().entrySet()) {
        Parameter evaluated =
            paramEvaluator.parseAttribute(
                entry.getValue(), Collections.emptyMap(), execution.getWorkflowId(), true);
        if (evaluated == null) {
          LOG.info(
              "Failed to evaluate the run param [{}] for workflow trigger [{}][{}] and skip the execution",
              entry.getValue(),
              workflowId,
              triggerUuid);
          metrics.counter(
              MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
              getClass(),
              MetricConstants.TYPE_TAG,
              "param_eval_error");
          return;
        }
        evaluatedParams.put(entry.getKey(), evaluated);
      }
    }
    String conditionExpr = execution.getCondition();
    if (conditionExpr != null && !conditionExpr.isEmpty()) {
      Parameter evaluated =
          paramEvaluator.parseAttribute(
              BooleanParamDefinition.builder()
                  .name("maestro_signal_condition")
                  .expression(conditionExpr)
                  .build(),
              Collections.emptyMap(),
              execution.getWorkflowId(),
              true);
      if (evaluated == null) {
        LOG.info(
            "Failed to evaluate the condition [{}] for workflow signal trigger [{}][{}] and skip the execution",
            conditionExpr,
            workflowId,
            triggerUuid);
        metrics.counter(
            MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
            getClass(),
            MetricConstants.TYPE_TAG,
            "condition_eval_error");
        return;
      }
      if (!evaluated.asBoolean()) {
        LOG.info(
            "The condition [{}] for workflow signal trigger [{}][{}] is evaluated as FALSE and skip the execution",
            conditionExpr,
            workflowId,
            triggerUuid);
        metrics.counter(
            MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
            getClass(),
            MetricConstants.TYPE_TAG,
            "false_condition");
        return;
      }
    }

    try {
      execution.setParams(null);
      UUID requestId = deriveRequestId(execution);
      RunResponse resp =
          actionHandler.start(
              execution.getWorkflowId(),
              Constants.WorkflowVersion.ACTIVE.name(),
              createWorkflowRunRequest(requestId, execution, evaluatedParams));
      LOG.info("signal trigger workflow run response is [{}]", resp);
      metrics.counter(MetricConstants.SIGNAL_TRIGGER_EXECUTION_SUCCESS, getClass());
    } catch (MaestroResourceConflictException ce) {
      metrics.counter(
          MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
          getClass(),
          MetricConstants.TYPE_TAG,
          "conflict");
      LOG.info(
          "Trigger uuid [{}] for workflow [{}] has been changed, deleting workflow trigger due to [{}]",
          triggerUuid,
          workflowId,
          ce.getMessage());
      brokerDao.deleteTrigger(workflowId, triggerUuid);
    } catch (MaestroNotFoundException ne) {
      handleMaestroNotFoundException(ne, workflowId, triggerUuid);
      brokerDao.deleteTrigger(workflowId, triggerUuid);
    } catch (MaestroUnprocessableEntityException e) {
      handleMaestroUnProcessableEntityException(e, workflowId, triggerUuid);
      brokerDao.deleteTrigger(workflowId, triggerUuid);
    } catch (RuntimeException re) {
      LOG.warn(
          "recoverable workflow trigger exception for [{}][{}] due to",
          workflowId,
          triggerUuid,
          re);
      metrics.counter(
          MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
          getClass(),
          MetricConstants.TYPE_TAG,
          "recoverable");
      throw new MaestroRetryableError(
          re, "recoverable workflow trigger exception for [%s][%s]", workflowId, triggerUuid);
    }
  }

  private UUID deriveRequestId(SignalTriggerExecution execution) {
    String uniqueStr = null;
    String dedupExpr = execution.getDedupExpr();
    if (dedupExpr != null && !dedupExpr.isEmpty()) {
      Parameter evaluated =
          paramEvaluator.parseAttribute(
              StringParamDefinition.builder()
                  .name("maestro_signal_dedup")
                  .expression(dedupExpr)
                  .build(),
              Collections.emptyMap(),
              execution.getWorkflowId(),
              true);
      if (evaluated == null) {
        LOG.info(
            "Failed to evaluate the dedup expr [{}] for workflow signal trigger [{}][{}] and default to null",
            dedupExpr,
            execution.getWorkflowId(),
            execution.getTriggerUuid());
      } else {
        uniqueStr = evaluated.getEvaluatedResultString();
      }
    }
    if (uniqueStr == null) {
      try {
        uniqueStr = objectMapper.writeValueAsString(execution);
      } catch (JsonProcessingException je) {
        LOG.warn(
            "Failed to create request id for signal trigger execution [{}] and default to null due to",
            execution,
            je);
      }
    }
    return IdHelper.createUuid(uniqueStr);
  }

  private RunRequest createWorkflowRunRequest(
      UUID requestId, SignalTriggerExecution execution, Map<String, Parameter> evaluatedParams) {
    SignalInitiator signalInitiator = new SignalInitiator();
    signalInitiator.setTriggerUuid(execution.getTriggerUuid());
    signalInitiator.setSignalIdMap(execution.getSignalIds());
    signalInitiator.setParams(evaluatedParams);
    return RunRequest.builder()
        .initiator(signalInitiator)
        .requestTime(System.currentTimeMillis())
        .requestId(requestId)
        .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
        .runParams(Collections.emptyMap())
        .persistFailedRun(true)
        .build();
  }

  private void handleMaestroNotFoundException(
      MaestroNotFoundException e, String workflowId, String triggerUuid) {
    if (ExceptionClassifier.isWorkflowNotFoundException(e)) {
      metrics.counter(
          MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
          getClass(),
          MetricConstants.TYPE_TAG,
          "not_found");
      LOG.info(
          "Trigger uuid [{}] for workflow [{}] has been deleted, deleting workflow trigger due to [{}]",
          triggerUuid,
          workflowId,
          e.getMessage());
    } else if (ExceptionClassifier.isNoActiveWorkflowVersionException(e)) {
      metrics.counter(
          MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
          getClass(),
          MetricConstants.TYPE_TAG,
          "inactive");
      LOG.info(
          "Trigger uuid [{}] for workflow [{}] has been deactivated, deleting workflow trigger due to [{}]",
          triggerUuid,
          workflowId,
          e.getMessage());
    } else {
      metrics.counter(
          MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
          getClass(),
          MetricConstants.TYPE_TAG,
          "unknown_not_found");
      LOG.info(
          "Unknown not_found error for workflow trigger [{}][{}]. will retry it due to",
          workflowId,
          triggerUuid,
          e);
      throw new MaestroRetryableError(
          e, "Unknown 404 not found error for workflow trigger [%s][%s]", workflowId, triggerUuid);
    }
  }

  private void handleMaestroUnProcessableEntityException(
      MaestroUnprocessableEntityException e, String workflowId, String triggerUuid) {
    if (ExceptionClassifier.isSignalTriggerDisabledException(e)) {
      metrics.counter(
          MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
          getClass(),
          MetricConstants.TYPE_TAG,
          "trigger_disabled");
      LOG.info(
          "Workflow trigger [{}][{}] has been disabled, deleting it due to [{}]",
          workflowId,
          triggerUuid,
          e.getMessage());
    } else {
      metrics.counter(
          MetricConstants.SIGNAL_TRIGGER_EXECUTION_FAILURE,
          getClass(),
          MetricConstants.TYPE_TAG,
          "unknown_unprocessable");
      LOG.info(
          "Unknown unprocessable error for workflow trigger [{}][{}]. Will retry it due to",
          workflowId,
          triggerUuid,
          e);
      throw new MaestroRetryableError(
          e,
          "Unknown unprocessable error for workflow trigger [%s][%s]. will retry it due to",
          workflowId,
          triggerUuid);
    }
  }
}
