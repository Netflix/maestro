package com.netflix.maestro.signal.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.signal.SignalMatchDto;
import com.netflix.maestro.models.signal.SignalParamDto;
import com.netflix.maestro.utils.IdHelper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.sql.DataSource;

/**
 * Maestro signal param dao to query maestro_signal_param table.
 *
 * @author jun-he
 */
public class MaestroSignalParamDao extends AbstractDatabaseDao {
  private static final String ADD_SIGNAL_PARAM_QUERY_TEMPLATE =
      "INSERT INTO maestro_signal_param (signal_name,param_name,encoded_val,signal_seq_id) VALUES %s ON CONFLICT DO NOTHING";
  private static final String VALUE_PLACE_HOLDER = "(?,?,?,?)";
  private static final String MATCH_SIGNAL_PARAM_QUERY_PREFIX =
      "SELECT signal_seq_id FROM maestro_signal_param WHERE signal_name=? AND param_name=? AND encoded_val";
  private static final String INTERSECT_JOIN = " INTERSECT ";
  private static final String LIMIT_ONE = " LIMIT 1";

  /** Constructor. */
  public MaestroSignalParamDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
  }

  /** Add signal params into the table. */
  int addSignalParams(Connection conn, List<SignalParamDto> signalParams, long seqId)
      throws SQLException {
    if (signalParams == null || signalParams.isEmpty()) {
      return 0;
    }
    String sql =
        String.format(
            ADD_SIGNAL_PARAM_QUERY_TEMPLATE,
            String.join(",", Collections.nCopies(signalParams.size(), VALUE_PLACE_HOLDER)));
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      int idx = 0;
      for (SignalParamDto signalParam : signalParams) {
        stmt.setString(++idx, signalParam.signalName());
        stmt.setString(++idx, signalParam.paramName());
        stmt.setString(++idx, signalParam.encodedValue());
        stmt.setLong(++idx, seqId);
      }
      return stmt.executeUpdate();
    }
  }

  /**
   * Match signal dependency with non-empty paramMatch list.
   *
   * @param signalMatch signal dependency with non-empty paramMatch list
   * @return signal sequence id if matched. null otherwise.
   */
  Long matchSignalDependency(SignalMatchDto signalMatch) {
    var queryParams = new ArrayList<String>();
    String sql = getSqlWithQueryParam(signalMatch, 0, queryParams);
    return withMetricLogError(
        () -> withRetryableTransaction(conn -> runSignalMatchQuery(conn, sql, queryParams)),
        "matchSignalDependency",
        "Failed to match the signal step dependency for [{}]",
        signalMatch);
  }

  /**
   * Match signal params with non-empty paramMatch list since a checkpoint.
   *
   * @param conn db connection
   * @param signalMatch signal matching request with non-empty paramMatch list
   * @param checkpoint signal sequence id checkpoint
   * @return signal sequence id if matched, null otherwise.
   * @throws SQLException sql error
   */
  Long matchSignal(Connection conn, SignalMatchDto signalMatch, long checkpoint)
      throws SQLException {
    var queryParams = new ArrayList<String>();
    String sql = getSqlWithQueryParam(signalMatch, checkpoint, queryParams);
    return runSignalMatchQuery(conn, sql, queryParams);
  }

  private Long runSignalMatchQuery(Connection conn, String sql, List<String> queryParams)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      int idx = 0;
      for (String queryParam : queryParams) {
        stmt.setString(++idx, queryParam);
      }
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getLong(1);
        }
        return null;
      }
    }
  }

  private String getSqlWithQueryParam(
      SignalMatchDto signalMatch, long checkpoint, List<String> queryParams) {
    var signalName = signalMatch.signalName();
    var signalMatches = new ArrayList<>(signalMatch.paramMatches());
    signalMatches.sort(SignalMatchDto.ParamMatchDto::compareTo);
    var paramMatchIterator = signalMatches.iterator();

    StringBuilder sqlBuilder =
        new StringBuilder(toSql(signalName, paramMatchIterator.next(), checkpoint, queryParams));
    while (paramMatchIterator.hasNext()) {
      sqlBuilder
          .append(INTERSECT_JOIN)
          .append(toSql(signalName, paramMatchIterator.next(), checkpoint, queryParams));
    }
    return sqlBuilder.append(LIMIT_ONE).toString();
  }

  private String toSql(
      String signalName,
      SignalMatchDto.ParamMatchDto paramMatch,
      long checkpoint,
      List<String> queryParams) {
    boolean isNumeric = paramMatch.paramValue().isLong();
    String encodedValue = IdHelper.encodeValue(paramMatch.paramValue());
    String condition =
        switch (paramMatch.operator()) {
          case EQUALS_TO -> "";
          case LESS_THAN, LESS_THAN_EQUALS_TO -> isNumeric ? ">'0' AND encoded_val" : "";
          case GREATER_THAN, GREATER_THAN_EQUALS_TO -> isNumeric ? "" : "<'0' AND encoded_val";
        };
    queryParams.add(signalName);
    queryParams.add(paramMatch.paramName());
    queryParams.add(encodedValue);
    String sql =
        MATCH_SIGNAL_PARAM_QUERY_PREFIX + condition + paramMatch.operator().getOperatorCode() + "?";
    if (checkpoint > 0) {
      sql = sql + " AND signal_seq_id>" + checkpoint;
    }
    return sql;
  }
}
