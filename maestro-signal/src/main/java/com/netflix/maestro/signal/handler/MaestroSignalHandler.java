package com.netflix.maestro.signal.handler;

import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.SignalHandler;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.api.SignalCreateRequest;
import com.netflix.maestro.models.instance.StepDependencyMatchStatus;
import com.netflix.maestro.models.signal.SignalDependencies;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.models.SignalInstanceSource;
import com.netflix.maestro.signal.models.SignalMatchDto;
import com.netflix.maestro.utils.ObjectHelper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro signal handler implementation using {@link MaestroSignalBrokerDao} dao.
 *
 * @author jun-he
 */
@Slf4j
@AllArgsConstructor
public class MaestroSignalHandler implements SignalHandler {
  private final MaestroSignalBrokerDao brokerDao;

  /**
   * Sends output signals of a step.
   *
   * @param workflowSummary workflow summary
   * @param stepRuntimeSummary the step runtime summary
   * @return true if sending the output signals task is done. Otherwise, false.
   */
  @Override
  @SuppressWarnings({
    "PMD.AvoidInstantiatingObjectsInLoops",
    "PMD.AvoidInstanceofChecksInCatchClause"
  })
  public boolean sendOutputSignals(
      WorkflowSummary workflowSummary, StepRuntimeSummary stepRuntimeSummary) {
    if (stepRuntimeSummary.getSignalOutputs() != null) {
      var signalOutputs = stepRuntimeSummary.getSignalOutputs().getOutputs();
      if (ObjectHelper.isCollectionEmptyOrNull(signalOutputs)) {
        return true;
      }
      for (var output : signalOutputs) {
        if (output.getSignalId() != null) {
          LOG.info(
              "Signal with name [{}] has already been sent with signal sequence id [{}], skip it",
              output.getName(),
              output.getSignalId());
          continue;
        }
        try {
          SignalCreateRequest request = new SignalCreateRequest();
          request.setName(output.getName());
          request.setParams(output.getParams());
          request.addPayload(
              "signal_source", SignalInstanceSource.create(workflowSummary, stepRuntimeSummary));

          SignalInstance instance = brokerDao.addSignal(request);
          output.setSignalId(instance.getSeqId());
          if (instance.getDetails() == null) {
            output.setAnnounceTime(instance.getCreateTime());
          }
          stepRuntimeSummary.flagToSync();
        } catch (RuntimeException e) { // always retry
          LOG.warn(
              "will retry as getting exception when sending output signal [{}] for workflow step {}{} due to ",
              output.getName(),
              workflowSummary.getIdentity(),
              stepRuntimeSummary.getIdentity(),
              e);
          if (e instanceof MaestroRetryableError) {
            throw e;
          }
          throw new MaestroRetryableError(
              e,
              "error when sending output signal [%s] for workflow step %s%s",
              output.getName(),
              workflowSummary.getIdentity(),
              stepRuntimeSummary.getIdentity());
        }
      }
      boolean done = signalOutputs.stream().noneMatch(so -> so.getSignalId() == null);
      if (done) {
        stepRuntimeSummary
            .getSignalOutputs()
            .setInfo(TimelineLogEvent.info("Signal step outputs have been completed."));
      }
    }
    return true;
  }

  /**
   * Checks the signal status of a StepRuntimeSummary. This method also updates the {@link
   * StepRuntimeSummary} if there is an update for any signals. It is currently based on a polling
   * mechanism. If needed, it can support pushing by waking up the step to check signal
   * dependencies.
   *
   * @param workflowSummary the workflow summary
   * @param stepRuntimeSummary the step runtime summary
   * @return true if the signals are ready, false otherwise
   */
  @Override
  public boolean signalsReady(
      WorkflowSummary workflowSummary, StepRuntimeSummary stepRuntimeSummary) {
    if (stepRuntimeSummary == null || stepRuntimeSummary.getSignalDependencies() == null) {
      return true;
    }
    SignalDependencies dependencies = stepRuntimeSummary.getSignalDependencies();
    if (ObjectHelper.isCollectionEmptyOrNull(dependencies.getDependencies())) {
      return true;
    }
    boolean changed = false;
    for (var dependency : dependencies.getDependencies()) {
      if (!dependency.getStatus().isDone()) {
        SignalMatchDto signalMatch = toSignalMatch(dependency);
        Long matchedSignal = brokerDao.matchSignalForStepDependency(signalMatch);
        if (matchedSignal != null) {
          dependency.update(matchedSignal, StepDependencyMatchStatus.MATCHED);
          changed = true;
        }
      }
    }

    if (changed) {
      stepRuntimeSummary.flagToSync();
    }
    return dependencies.isSatisfied();
  }

  /** params will not be null and contain at least a `name` param. */
  private SignalMatchDto toSignalMatch(SignalDependencies.SignalDependency dependency) {
    String signalName = dependency.getName();
    var paramMatches =
        dependency.getMatchParams().entrySet().stream()
            .map(
                e ->
                    new SignalMatchDto.ParamMatchDto(
                        e.getKey(), e.getValue().getValue(), e.getValue().getOperator()))
            .toList();
    return new SignalMatchDto(signalName, paramMatches);
  }

  /**
   * Get the parameters from all signal dependencies.
   *
   * @param signalName the signal name
   * @param signalId the signal seq id
   * @return the list of signal param maps
   */
  @Override
  public SignalInstance getSignalInstance(String signalName, long signalId) {
    return brokerDao.getSignalInstance(signalName, signalId);
  }
}
