package com.netflix.maestro.flow.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.Constants;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowGroup;
import com.netflix.maestro.metrics.MaestroMetrics;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Maestro internal flow engine dao implementation.
 *
 * @author jun-he
 */
public class MaestroFlowDao extends AbstractDatabaseDao {

  private static final String ADD_FLOW_QUERY =
      "INSERT INTO maestro_flow (group_id,flow_id,generation,start_time,reference) "
          + "SELECT ?,?,?,?,? FROM maestro_flow_group WHERE group_id=? AND generation=? "
          + "ON CONFLICT DO NOTHING";
  private static final String REMOVE_FLOW_QUERY =
      "DELETE FROM maestro_flow WHERE group_id=? AND flow_id=?";
  private static final String CLAIM_AND_RETURN_FLOWS_QUERY =
      "UPDATE maestro_flow SET generation=? WHERE group_id=? AND flow_id IN ("
          + "SELECT flow_id FROM maestro_flow WHERE group_id=? AND flow_id>? AND generation<? "
          + "ORDER BY flow_id ASC LIMIT ?) RETURNING flow_id,start_time,reference";
  private static final String HEARTBEAT_QUERY =
      "UPDATE maestro_flow_group SET heartbeat_ts=now() WHERE group_id=? AND generation=? RETURNING heartbeat_ts";
  private static final String RELEASE_GROUP_QUERY =
      "UPDATE maestro_flow_group SET heartbeat_ts=TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00' WHERE group_id=? AND generation=?";
  private static final String CLAIM_FLOW_GROUP_QUERY =
      "UPDATE maestro_flow_group SET (heartbeat_ts,generation,address)=(now(),generation+1,?) "
          + "WHERE group_id=(SELECT group_id FROM maestro_flow_group "
          + "WHERE heartbeat_ts<(now()-INTERVAL '1 millisecond' * ?) FOR UPDATE SKIP LOCKED LIMIT 1) "
          + "RETURNING group_id,generation,heartbeat_ts";
  private static final String ADD_FLOW_GROUP_QUERY =
      "INSERT INTO maestro_flow_group (group_id,generation,address) VALUES (?,?,?) ON CONFLICT DO NOTHING RETURNING heartbeat_ts";
  private static final String GET_FLOW_WITH_SAME_KEYS_QUERY =
      "SELECT 1 FROM maestro_flow WHERE group_id=? AND flow_id=? LIMIT 1";
  private static final String REMOVE_GROUP_QUERY =
      "DELETE FROM maestro_flow_group WHERE group_id=?";
  private static final String GET_GROUP_QUERY =
      "SELECT generation,address,heartbeat_ts FROM maestro_flow_group WHERE group_id=?";

  public MaestroFlowDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
  }

  /**
   * Insert the flow into the flow table. It ensures the current flow group generation is correct so
   * making sure the ownership is not expired. There is no need to lock the group. Then it ensures
   * the flow will guarantee to be loaded by a flow group owner as the new owner will load
   * everything.
   *
   * @param flow flow to insert
   */
  public void insertFlow(Flow flow) {
    int res =
        withMetricLogError(
            () ->
                withRetryableUpdate(
                    ADD_FLOW_QUERY,
                    stmt -> {
                      int idx = 0;
                      stmt.setLong(++idx, flow.getGroupId());
                      stmt.setString(++idx, flow.getFlowId());
                      stmt.setLong(++idx, flow.getGeneration());
                      stmt.setLong(++idx, flow.getStartTime());
                      stmt.setString(++idx, flow.getReference());
                      stmt.setLong(++idx, flow.getGroupId());
                      stmt.setLong(++idx, flow.getGeneration());
                    }),
            "insertFlow",
            "Failed to insert the flow for the reference [{}]",
            flow.getReference());
    if (res != 1) {
      throw new MaestroRetryableError(
          "insertFlow for flow [%s] is failed (res=[%s]) and please retry",
          flow.getReference(), res);
    }
  }

  /**
   * Delete the flow after it is completed. Note that flow table cannot provide uniqueness guarantee
   * due to this. A flow can be created and then completed and then deleted. Then the same flow
   * might be running after and then cause a duplicate execution. Its caller's responsibility to
   * ensure it won't happen. Otherwise, the caller should disable delete flow and then do the
   * cleanup after a certain period of time.
   *
   * @param flow flow to delete
   */
  public void deleteFlow(Flow flow) {
    withMetricLogError(
        () ->
            withRetryableUpdate(
                REMOVE_FLOW_QUERY,
                stmt -> {
                  stmt.setLong(1, flow.getGroupId());
                  stmt.setString(2, flow.getFlowId());
                }),
        "deleteFlow",
        "Failed to delete the flow for the reference [{}]",
        flow.getReference());
  }

  /**
   * Get a batch of flows for a given flow group from the provided flow id cursor.
   *
   * @param group flow group to fill
   * @param limit limit in one batch
   * @param idCursor id cursor starts from '' empty string
   * @return the list of flows
   */
  public List<Flow> getFlows(FlowGroup group, long limit, String idCursor) {
    List<Flow> flows = new ArrayList<>();
    return withMetricLogError(
        () ->
            withRetryableQuery(
                CLAIM_AND_RETURN_FLOWS_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setLong(++idx, group.generation());
                  stmt.setLong(++idx, group.groupId());
                  stmt.setLong(++idx, group.groupId());
                  stmt.setString(++idx, idCursor);
                  stmt.setLong(++idx, group.generation());
                  stmt.setLong(++idx, limit);
                },
                result -> {
                  while (result.next()) {
                    int idx = 0;
                    flows.add(
                        new Flow(
                            group.groupId(),
                            result.getString(++idx),
                            group.generation(),
                            result.getLong(++idx),
                            result.getString(++idx)));
                  }
                  return flows;
                }),
        "getFlows",
        "Failed to getFlows for flow group [{}] with limit [{}] and idCursor [{}]",
        group.groupId(),
        limit,
        idCursor);
  }

  /** Used to get if there is any flow instance with this flow keys. */
  public boolean existFlowWithSameKeys(long groupId, String flowId) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_FLOW_WITH_SAME_KEYS_QUERY,
                stmt -> {
                  stmt.setLong(1, groupId);
                  stmt.setString(2, flowId);
                },
                ResultSet::next),
        "existFlowWithSameKeys",
        "Failed to check the existence of the flow instance [{}][{}]",
        groupId,
        flowId);
  }

  /**
   * It heartbeats to keep the ownership of the flow group. It also detects if the ownership is lost
   * unexpectedly. It returns the flag (if the ownership is valid) to the caller.
   *
   * @param group flow group info
   * @return the heartbeat timestamp, if not null, indicating the group ownership is still valid
   */
  public Long heartbeatGroup(FlowGroup group) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                HEARTBEAT_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setLong(++idx, group.groupId());
                  stmt.setLong(++idx, group.generation());
                },
                result -> {
                  if (result.next()) {
                    return result.getTimestamp(1).getTime();
                  }
                  return null;
                }),
        "heartbeatGroup",
        "Failed to heartbeat the flow group for [{}]",
        group.groupId());
  }

  public void releaseGroup(FlowGroup group) {
    withMetricLogError(
        () ->
            withRetryableUpdate(
                RELEASE_GROUP_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setLong(++idx, group.groupId());
                  stmt.setLong(++idx, group.generation());
                }),
        "releaseGroup",
        "Failed to release the flow group for [{}]",
        group);
  }

  /**
   * Claim the expired ownership of a flow group. Note that it might claim the same expired group
   * again but will have a different generation.
   *
   * @param address owner's unique address
   * @param expiration expiration duration in milliseconds to check
   * @return claimed flow group
   */
  public FlowGroup claimExpiredGroup(String address, long expiration) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                CLAIM_FLOW_GROUP_QUERY,
                stmt -> {
                  stmt.setString(1, address);
                  stmt.setLong(2, expiration);
                },
                result -> {
                  if (result.next()) {
                    int idx = 0;
                    return new FlowGroup(
                        result.getLong(++idx),
                        result.getLong(++idx),
                        address,
                        result.getTimestamp(++idx).getTime());
                  }
                  return null;
                }),
        "claimGroup",
        "Failed to claim a group by an owner with an address [{}]",
        address);
  }

  /**
   * Create a new brand new empty group.
   *
   * @param groupId new flow group id
   * @param address new flow group owner address
   * @return the added flow group
   */
  public FlowGroup insertGroup(long groupId, String address) {
    Long heartbeatTs =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    ADD_FLOW_GROUP_QUERY,
                    stmt -> {
                      int idx = 0;
                      stmt.setLong(++idx, groupId);
                      stmt.setLong(++idx, Constants.INITIAL_GENERATION_NUMBER);
                      stmt.setString(++idx, address);
                    },
                    result -> {
                      if (result.next()) {
                        return result.getTimestamp(1).getTime();
                      }
                      return null;
                    }),
            "insertGroup",
            "Failed to insert the flow group for group id [{}]",
            groupId);
    if (heartbeatTs == null) {
      throw new MaestroRetryableError(
          "insertGroup for group-[%s] has a conflict and please retry", groupId);
    }
    return new FlowGroup(groupId, Constants.INITIAL_GENERATION_NUMBER, address, heartbeatTs);
  }

  // return null if not found and then run it locally
  public FlowGroup getGroup(long groupId) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_GROUP_QUERY,
                stmt -> stmt.setLong(1, groupId),
                result -> {
                  if (result.next()) {
                    int idx = 0;
                    return new FlowGroup(
                        groupId,
                        result.getLong(++idx),
                        result.getString(++idx),
                        result.getTimestamp(++idx).getTime());
                  }
                  return null;
                }),
        "getGroup",
        "Failed to get the group for the groupId [{}]",
        groupId);
  }

  @VisibleForTesting
  void deleteGroup(long groupId) {
    withMetricLogError(
        () -> withRetryableUpdate(REMOVE_GROUP_QUERY, stmt -> stmt.setLong(1, groupId)),
        "deleteGroup",
        "Failed to delete the group for the groupId [{}]",
        groupId);
  }
}
