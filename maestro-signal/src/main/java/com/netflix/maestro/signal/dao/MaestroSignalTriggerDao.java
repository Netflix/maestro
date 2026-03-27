package com.netflix.maestro.signal.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.signal.SignalTriggerDto;
import com.netflix.maestro.models.signal.SignalTriggerMatch;
import com.netflix.maestro.models.trigger.SignalTrigger;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Maestro signal trigger dao to query maestro_signal_trigger table.
 *
 * @author jun-he
 */
public class MaestroSignalTriggerDao extends AbstractDatabaseDao {
  private static final String ADD_SIGNAL_TRIGGER_QUERY =
      "INSERT INTO maestro_signal_trigger (workflow_id,trigger_uuid,definition,signals,checkpoints) "
          + "SELECT ?,?,?,ARRAY_AGG(nm ORDER BY nm),ARRAY_AGG(seq ORDER BY seq) FROM ("
          + "SELECT f.n as nm,COALESCE(sid,0) as seq FROM (SELECT UNNEST(?) as n) f LEFT JOIN ("
          + "SELECT name,max(seq_id) as sid FROM maestro_signal_instance WHERE name=ANY(?) GROUP BY name"
          + ") si ON f.n=si.name) ON CONFLICT DO NOTHING";
  private static final String GET_SIGNAL_TRIGGER_QUERY =
      "SELECT workflow_id,trigger_uuid FROM maestro_signal_trigger WHERE signals @> ARRAY[?::TEXT]";
  private static final String GET_SIGNAL_TRIGGER_FOR_UPDATE_QUERY =
      "SELECT definition,signals,checkpoints FROM maestro_signal_trigger WHERE workflow_id=? AND trigger_uuid=? FOR UPDATE";
  private static final String DELETE_SIGNAL_TRIGGER_QUERY =
      "DELETE FROM maestro_signal_trigger WHERE workflow_id=? AND trigger_uuid=?";
  private static final String UPDATE_SIGNAL_TRIGGER_CHECKPOINTS_QUERY =
      "UPDATE maestro_signal_trigger SET checkpoints=? WHERE workflow_id=? AND trigger_uuid=?";

  /** Constructor. */
  public MaestroSignalTriggerDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
  }

  /**
   * subscribe to the workflow signal trigger including signal names in the trigger. It also saves
   * the current latest signal sequence ids as the checkpoints for each signal name.
   *
   * @param conn db connection
   * @param workflowId workflow id
   * @param triggerUuid trigger uuid
   * @param signalTrigger signal trigger
   * @return true if added, false otherwise.
   * @throws SQLException sql error
   */
  boolean addWorkflowTrigger(
      Connection conn, String workflowId, String triggerUuid, SignalTrigger signalTrigger)
      throws SQLException {
    String definition = toJson(signalTrigger);
    Object[] signalNames = signalTrigger.getDefinitions().keySet().toArray();
    try (PreparedStatement stmt = conn.prepareStatement(ADD_SIGNAL_TRIGGER_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, workflowId);
      stmt.setString(++idx, triggerUuid);
      stmt.setString(++idx, definition);
      stmt.setArray(++idx, conn.createArrayOf(ARRAY_TYPE_NAME, signalNames));
      stmt.setArray(++idx, conn.createArrayOf(ARRAY_TYPE_NAME, signalNames));
      return stmt.executeUpdate() == SUCCESS_WRITE_SIZE;
    }
  }

  /**
   * Get workflow triggers subscribing to the given signal name.
   *
   * @param signalName signal name
   * @return a list of triggers matched.
   */
  List<SignalTriggerMatch> getSubscribedTriggers(String signalName) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_SIGNAL_TRIGGER_QUERY,
                stmt -> stmt.setString(1, signalName),
                rs -> {
                  List<SignalTriggerMatch> refs = new ArrayList<>();
                  while (rs.next()) {
                    refs.add(fromResult(rs));
                  }
                  return refs;
                }),
        "getSubscribedTriggers",
        "Failed to get the related workflow signal triggers for signal name [{}]",
        signalName);
  }

  private SignalTriggerMatch fromResult(ResultSet rs) throws SQLException {
    SignalTriggerMatch match = new SignalTriggerMatch();
    match.setWorkflowId(rs.getString(1));
    match.setTriggerUuid(rs.getString(2));
    return match;
  }

  /** Get signal trigger with the lock by using select for update. */
  SignalTriggerDto getTriggerForUpdate(Connection conn, String workflowId, String triggerUuid)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(GET_SIGNAL_TRIGGER_FOR_UPDATE_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, workflowId);
      stmt.setString(++idx, triggerUuid);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          idx = 0;
          String def = rs.getString(++idx);
          Array signalsArray = rs.getArray(++idx);
          Array checkpointsArray = rs.getArray(++idx);
          try {
            return new SignalTriggerDto(
                workflowId,
                triggerUuid,
                def,
                (String[]) signalsArray.getArray(),
                (Long[]) checkpointsArray.getArray());
          } finally {
            signalsArray.free();
            checkpointsArray.free();
          }
        }
        return null; // not existing
      }
    }
  }

  /**
   * Delete a signal trigger.
   *
   * @param workflowId workflow id
   * @param triggerUuid trigger uuid
   */
  void delete(String workflowId, String triggerUuid) {
    withMetricLogError(
        () ->
            withRetryableUpdate(
                DELETE_SIGNAL_TRIGGER_QUERY,
                stmt -> {
                  stmt.setString(1, workflowId);
                  stmt.setString(2, triggerUuid);
                }),
        "delete",
        "Failed to delete the workflow signal trigger for [{}][{}]",
        workflowId,
        triggerUuid);
  }

  /** Update trigger checkpoints. */
  @SuppressWarnings("PMD.UseVarargs")
  boolean updateTriggerCheckpoints(
      Connection conn, String workflowId, String triggerUuid, Long[] checkpoints)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(UPDATE_SIGNAL_TRIGGER_CHECKPOINTS_QUERY)) {
      int idx = 0;
      stmt.setArray(++idx, conn.createArrayOf("INT8", checkpoints));
      stmt.setString(++idx, workflowId);
      stmt.setString(++idx, triggerUuid);
      return stmt.executeUpdate() == SUCCESS_WRITE_SIZE;
    }
  }
}
