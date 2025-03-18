package com.netflix.maestro.signal.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.signal.models.SignalInstanceRef;
import com.netflix.maestro.signal.models.SignalMatchDto;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Maestro signal instance dao to query maestro_signal_instance table.
 *
 * @author jun-he
 */
public class MaestroSignalInstanceDao extends AbstractDatabaseDao {
  private static final String ADD_SIGNAL_INSTANCE_QUERY =
      "INSERT INTO maestro_signal_instance (name,seq_id,instance_id,instance) "
          + "SELECT f.n,COALESCE(si.seq_id,0)+1,?,? FROM (SELECT ? as n) f LEFT JOIN "
          + "(SELECT name,seq_id FROM maestro_signal_instance WHERE name=? ORDER BY name,seq_id DESC LIMIT 1) si "
          + "ON f.n=si.name ON CONFLICT (name,instance_id) DO NOTHING RETURNING seq_id";
  private static final String MATCH_SIGNAL_DEPENDENCY_QUERY =
      "SELECT seq_id FROM maestro_signal_instance WHERE name=? LIMIT 1";
  private static final String MATCH_SIGNAL_QUERY =
      "SELECT seq_id FROM maestro_signal_instance WHERE name=? AND seq_id>? ORDER BY seq_id ASC LIMIT 1";
  private static final String GET_SIGNAL_INSTANCE_QUERY =
      "SELECT instance FROM maestro_signal_instance WHERE name=? AND seq_id=?";
  private static final String GET_LATEST_SIGNAL_INSTANCE_QUERY =
      "SELECT instance FROM maestro_signal_instance WHERE name=? ORDER BY seq_id DESC LIMIT 1";

  /** Constructor. */
  public MaestroSignalInstanceDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
  }

  /**
   * Add a signal instance to db.
   *
   * @param conn db connection
   * @param instanceId signal instance id for deduplication
   * @param name signal name
   * @param instanceStr signal instance serialized string
   * @return the created signal instance sequence id
   * @throws SQLException sql error
   */
  long addSignalInstance(Connection conn, String instanceId, String name, String instanceStr)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(ADD_SIGNAL_INSTANCE_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, instanceId);
      stmt.setString(++idx, instanceStr);
      stmt.setString(++idx, name);
      stmt.setString(++idx, name);
      try (ResultSet result = stmt.executeQuery()) {
        if (result.next()) {
          return result.getLong(1);
        }
        return -1; // signal instance already exists
      }
    }
  }

  /** Get signal instance for a given reference. */
  SignalInstance getSignalInstance(SignalInstanceRef ref) {
    if (ref == null) {
      return null;
    }
    String instanceStr =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    ref.signalId() >= 0
                        ? GET_SIGNAL_INSTANCE_QUERY
                        : GET_LATEST_SIGNAL_INSTANCE_QUERY,
                    stmt -> {
                      stmt.setString(1, ref.signalName());
                      if (ref.signalId() >= 0) {
                        stmt.setLong(2, ref.signalId());
                      }
                    },
                    rs -> {
                      if (rs.next()) {
                        return rs.getString(1);
                      }
                      return null;
                    }),
            "getSignalInstance",
            "Failed to get the related signal instance for signal instance ref [{}]",
            ref);
    if (instanceStr == null) {
      return null;
    }
    SignalInstance instance = fromJson(instanceStr, SignalInstance.class);
    instance.setSeqId(ref.signalId());
    return instance;
  }

  /**
   * Match signal dependency with null or empty paraMatch list.
   *
   * @param signalMatch signal dependency with null or empty paramMatch list
   * @return signal sequence id if matched, null otherwise.
   */
  Long matchSignalDependency(SignalMatchDto signalMatch) {
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn ->
                    runSignalMatchQuery(
                        conn, MATCH_SIGNAL_DEPENDENCY_QUERY, signalMatch.signalName(), -1)),
        "matchSignalDependency",
        "Failed to match the signal step dependency for [{}]",
        signalMatch);
  }

  /**
   * Match signal dependency with null or empty paramMatch list since a checkpoint.
   *
   * @param conn db connection
   * @param signalMatch signal dependency with null or empty paramMatch list
   * @param checkpoint signal sequence id checkpoint
   * @return signal instance id if matched, null otherwise
   * @throws SQLException sql error
   */
  Long matchSignal(Connection conn, SignalMatchDto signalMatch, long checkpoint)
      throws SQLException {
    return runSignalMatchQuery(conn, MATCH_SIGNAL_QUERY, signalMatch.signalName(), checkpoint);
  }

  private Long runSignalMatchQuery(Connection conn, String sql, String signalName, long checkpoint)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, signalName);
      if (checkpoint >= 0) {
        stmt.setLong(2, checkpoint);
      }
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getLong(1);
        }
        return null;
      }
    }
  }
}
