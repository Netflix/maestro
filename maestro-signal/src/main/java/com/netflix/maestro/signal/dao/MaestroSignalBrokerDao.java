package com.netflix.maestro.signal.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.api.SignalCreateRequest;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalOperator;
import com.netflix.maestro.models.signal.SignalParamValue;
import com.netflix.maestro.models.trigger.SignalTrigger;
import com.netflix.maestro.signal.models.SignalInstanceRef;
import com.netflix.maestro.signal.models.SignalMatchDto;
import com.netflix.maestro.signal.models.SignalParamDto;
import com.netflix.maestro.signal.models.SignalTriggerDef;
import com.netflix.maestro.signal.models.SignalTriggerDto;
import com.netflix.maestro.signal.models.SignalTriggerExecution;
import com.netflix.maestro.signal.models.SignalTriggerMatch;
import com.netflix.maestro.signal.producer.SignalQueueProducer;
import com.netflix.maestro.utils.IdHelper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a specialized Dao. It gives the transactional view on the top of multiple DAOs. No
 * underlying database operations are performed here.
 *
 * @author jun-he
 */
@Slf4j
public class MaestroSignalBrokerDao extends AbstractDatabaseDao {
  private final MaestroSignalInstanceDao instanceDao;
  private final MaestroSignalParamDao paramDao;
  private final MaestroSignalTriggerDao triggerDao;
  private final SignalQueueProducer queueProducer;

  /** Constructor. */
  public MaestroSignalBrokerDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics,
      MaestroSignalInstanceDao instanceDao,
      MaestroSignalParamDao paramDao,
      MaestroSignalTriggerDao triggerDao,
      SignalQueueProducer queueProducer) {
    super(dataSource, objectMapper, config, metrics);
    this.instanceDao = instanceDao;
    this.paramDao = paramDao;
    this.triggerDao = triggerDao;
    this.queueProducer = queueProducer;
  }

  /**
   * It extracts the fields from the signal request to create a cleaned request to generate signal
   * instance id. It sorts the params by their keys and then clear the request time.
   */
  private String getOrCreateInstanceId(SignalCreateRequest request) {
    if (request.getRequestId() != null) {
      return request.getRequestId();
    }
    SignalCreateRequest sorted = new SignalCreateRequest();
    sorted.setName(request.getName());
    if (request.getParams() != null) {
      sorted.setParams(new TreeMap<>(request.getParams()));
    }
    sorted.setRequestTime(0);
    // if needed, we can also consider payload when computing instance id.
    return IdHelper.createUuid(toJson(sorted)).toString();
  }

  // it does not include the signal instance sequence number as still unknown
  private List<SignalParamDto> createSignalParams(SignalCreateRequest request) {
    if (request.getParams() == null) {
      return null;
    }
    return request.getParams().entrySet().stream()
        .map(
            e ->
                new SignalParamDto(
                    request.getName(), e.getKey(), IdHelper.encodeValue(e.getValue())))
        .toList();
  }

  /** Add a signal instance based on the signal create request. */
  public SignalInstance addSignal(SignalCreateRequest request) {
    SignalInstance instance = SignalInstance.from(request, getOrCreateInstanceId(request));
    String instanceStr = toJson(instance);
    var signalParams = createSignalParams(request);

    boolean duplicate =
        withMetricLogError(
            () ->
                withRetryableTransaction(
                    conn -> {
                      long seqId =
                          instanceDao.addSignalInstance(
                              conn, instance.getInstanceId(), instance.getName(), instanceStr);
                      instance.setSeqId(seqId);
                      if (seqId > 0) {
                        paramDao.addSignalParams(conn, signalParams, seqId);
                        queueProducer.push(instance); // trigger subscription check
                        return false;
                      }
                      return true;
                    }),
            "addSignal",
            "Failed to add a new signal instance: [{}]",
            instanceStr);
    if (duplicate) {
      instance.setDetails(
          Details.create(
              new MaestroResourceConflictException(
                  "Signal instance [%s] already exists. If needed, please set a different request_id to announce it again",
                  instanceStr),
              false,
              "Duplicate signal instance"));
    }
    return instance;
  }

  /**
   * Get matched signal sequence id for the given signal step dependency. It is currently based on
   * polling and can support push if needed. The query only needs to hit the read replica if setup.
   *
   * @param signalMatch signal match object for a signal dependency
   * @return signal sequence id. If not found, return null.
   */
  public Long matchSignalForStepDependency(SignalMatchDto signalMatch) {
    if (signalMatch.withParams()) {
      return paramDao.matchSignalDependency(signalMatch);
    } else { // check signal instance dao if no param
      return instanceDao.matchSignalDependency(signalMatch);
    }
  }

  /**
   * Add a list of workflow signal triggers and subscribe the future signal announcements.
   *
   * @param conn db connection to provide transaction support
   * @param signalTriggers a list of signal triggers including signal trigger uuids
   * @throws SQLException sql error
   */
  public void subscribeWorkflowTriggers(Connection conn, List<SignalTriggerDef> signalTriggers)
      throws SQLException {
    for (SignalTriggerDef def : signalTriggers) {
      boolean created =
          triggerDao.addWorkflowTrigger(
              conn, def.workflowId(), def.triggerUuid(), def.signalTrigger());
      if (!created) { // short circuit to abort the transaction
        LOG.info("workflow trigger [{}] not added b/c signal trigger already exists", def);
        throw new MaestroResourceConflictException(
            "Failed to add workflow trigger [%s] due to duplicate trigger uuid", def);
      }
    }
  }

  /**
   * Get subscribed triggers for a given signal instance.
   *
   * @param signalInstance signal instance
   * @return a list of matched signal triggers
   */
  public List<SignalTriggerMatch> getSubscribedTriggers(SignalInstance signalInstance) {
    List<SignalTriggerMatch> matches = triggerDao.getSubscribedTriggers(signalInstance.getName());
    signalInstance.setPayload(null); // clean up for execution
    for (SignalTriggerMatch match : matches) {
      match.setSignalInstance(signalInstance);
    }
    return matches;
  }

  /**
   * Delete a signal trigger.
   *
   * @param workflowId workflow id
   * @param triggerUuid signal trigger uuid
   */
  public void deleteTrigger(String workflowId, String triggerUuid) {
    try {
      triggerDao.delete(workflowId, triggerUuid);
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to delete the workflow signal trigger and skip it for now. Will delete it in the future.",
          e);
    }
  }

  /**
   * Get a list of signal instances.
   *
   * @param refs signal instance references
   * @return a list of signal instances
   */
  public List<SignalInstance> getSignalInstances(List<SignalInstanceRef> refs) {
    return refs.stream().map(instanceDao::getSignalInstance).toList();
  }

  /**
   * Get a specific signal instance.
   *
   * @param signalName signal name
   * @param seqId sequence id
   * @return the signal instance
   */
  public SignalInstance getSignalInstance(String signalName, long seqId) {
    return instanceDao.getSignalInstance(new SignalInstanceRef(signalName, seqId));
  }

  /**
   * Get the latest signal instance for a signal name.
   *
   * @param signalName signal name
   * @return the signal instance
   */
  public SignalInstance getLatestSignalInstance(String signalName) {
    return instanceDao.getSignalInstance(new SignalInstanceRef(signalName, -1));
  }

  /**
   * Check if matched signal trigger indeed will cause a workflow execution.
   *
   * @param match matched signal trigger
   * @return 1 if going to execute a workflow, -1 if invalid, and 0 is unmatched.
   */
  public int tryExecuteTrigger(SignalTriggerMatch match) {
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  SignalInstance instance = match.getSignalInstance();
                  SignalTriggerDto triggerDto =
                      triggerDao.getTriggerForUpdate(
                          conn, match.getWorkflowId(), match.getTriggerUuid());
                  if (triggerDto == null || triggerDto.definition() == null) {
                    return -1; // not found valid trigger definition
                  }
                  for (int i = 0; i < triggerDto.signals().length; ++i) {
                    if (instance.getName().equals(triggerDto.signals()[i])) {
                      if (triggerDto.checkpoints()[i] >= instance.getSeqId()) {
                        return 0; // instance is no longer valid to match
                      }
                    }
                  }
                  SignalTrigger trigger = fromJson(triggerDto.definition(), SignalTrigger.class);
                  if (!isMatched(trigger, instance)) {
                    return 0;
                  }
                  var baseEntry = trigger.getDefinitions().get(instance.getName());
                  if (baseEntry == null) {
                    return 0; // instance is no longer valid to match
                  }
                  List<SignalParamValue> joinValues = new ArrayList<>();
                  if (baseEntry.getJoinKeys() != null) { // OK to be null, then no join values added
                    for (String k : baseEntry.getJoinKeys()) {
                      SignalParamValue joinValue = instance.getParams().get(k);
                      if (joinValue == null) {
                        return 0; // unmatched
                      }
                      joinValues.add(joinValue);
                    }
                  }

                  Long[] matchedIds = new Long[triggerDto.signals().length];
                  for (int i = 0; i < triggerDto.signals().length; ++i) {
                    if (instance.getName().equals(triggerDto.signals()[i])) {
                      matchedIds[i] = instance.getSeqId();
                    } else {
                      var signalMatch = buildMatch(joinValues, trigger, triggerDto.signals()[i]);
                      if (signalMatch == null) {
                        return 0; // unmatched, no execution
                      }
                      Long seqId =
                          matchSignalForTrigger(conn, signalMatch, triggerDto.checkpoints()[i]);
                      if (seqId == null) {
                        return 0; // unmatched, non execution
                      }
                      matchedIds[i] = seqId;
                    }
                  }
                  boolean updated =
                      triggerDao.updateTriggerCheckpoints(
                          conn, triggerDto.workflowId(), triggerDto.triggerUuid(), matchedIds);
                  if (!updated) {
                    throw new MaestroRetryableError(
                        "Failed to update the checkpoint for [%s], please try it again", match);
                  }
                  queueProducer.push(
                      buildExecution(match, triggerDto.signals(), matchedIds, trigger));
                  return 1;
                }),
        "tryExecuteTrigger",
        "Failed to process a trigger execution: [{}]",
        match);
  }

  /**
   * Check if signal instance matches signal trigger.
   *
   * @param trigger signal trigger
   * @param instance signal instance
   * @return true if matching, otherwise, false.
   */
  private boolean isMatched(SignalTrigger trigger, SignalInstance instance) {
    if (trigger == null
        || trigger.getDefinitions() == null
        || !trigger.getDefinitions().containsKey(instance.getName())) {
      return false;
    }
    var entry = trigger.getDefinitions().get(instance.getName());
    if (entry.getMatchParams() == null || entry.getMatchParams().isEmpty()) {
      return true;
    }
    if (instance.getParams() == null || instance.getParams().isEmpty()) {
      return false;
    }
    return entry.getMatchParams().entrySet().stream()
        .allMatch(
            e -> {
              SignalParamValue value = instance.getParams().get(e.getKey());
              if (value == null) {
                return false;
              }
              return e.getValue().isSatisfied(value);
            });
  }

  /**
   * Build SignalMatchDto object.
   *
   * @param joinValues join keys' values
   * @param trigger signal trigger
   * @param signalName signal name
   * @return SignalMatchDTO object
   */
  private SignalMatchDto buildMatch(
      List<SignalParamValue> joinValues, SignalTrigger trigger, String signalName) {
    var curr = trigger.getDefinitions().get(signalName);
    if (curr == null) {
      return null;
    }
    List<SignalMatchDto.ParamMatchDto> paramMatches = new ArrayList<>();
    if (curr.getMatchParams() != null && !curr.getMatchParams().isEmpty()) {
      for (var e : curr.getMatchParams().entrySet()) {
        paramMatches.add(
            new SignalMatchDto.ParamMatchDto(
                e.getKey(), e.getValue().getValue(), e.getValue().getOperator()));
      }
    }
    if (curr.getJoinKeys() != null && curr.getJoinKeys().length > 0) {
      if (curr.getJoinKeys().length != joinValues.size()) {
        LOG.warn("Invalid join keys as the size does not match");
        return null;
      }
      for (int i = 0; i < curr.getJoinKeys().length; ++i) {
        paramMatches.add(
            new SignalMatchDto.ParamMatchDto(
                curr.getJoinKeys()[i], joinValues.get(i), SignalOperator.EQUALS_TO));
      }
    }
    return new SignalMatchDto(signalName, paramMatches);
  }

  /**
   * Get matched signal sequence id for a signal within a signal trigger.
   *
   * @param conn db connection
   * @param signalMatch signal match object for a signal trigger
   * @param checkpoint checkpoint
   * @return signal sequence id. If not found, return null.
   * @throws SQLException sql error
   */
  private Long matchSignalForTrigger(Connection conn, SignalMatchDto signalMatch, long checkpoint)
      throws SQLException {
    if (signalMatch.withParams()) {
      return paramDao.matchSignal(conn, signalMatch, checkpoint);
    } else { // check signal instance dao if no param
      return instanceDao.matchSignal(conn, signalMatch, checkpoint);
    }
  }

  private SignalTriggerExecution buildExecution(
      SignalTriggerMatch match, String[] keys, Long[] values, SignalTrigger trigger) {
    var execution = new SignalTriggerExecution();
    execution.setWorkflowId(match.getWorkflowId());
    execution.setTriggerUuid(match.getTriggerUuid());
    var nameIdMap = new LinkedHashMap<String, Long>();
    for (int i = 0; i < keys.length; ++i) {
      nameIdMap.put(keys[i], values[i]);
    }
    execution.setSignalIds(nameIdMap);
    execution.setCondition(trigger.getCondition());
    execution.setDedupExpr(trigger.getDedupExpr());
    execution.setParams(trigger.getParams());
    return execution;
  }
}
