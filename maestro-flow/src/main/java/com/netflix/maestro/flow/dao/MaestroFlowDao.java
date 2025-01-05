package com.netflix.maestro.flow.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowGroup;
import com.netflix.maestro.flow.properties.FlowEngineProperties;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.utils.Checks;
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
          + "SELECT ?,?,?,?,? FROM maestro_flow_group WHERE group_id=? AND generation=?";
  private static final String REMOVE_FLOW_QUERY =
      "DELETE FROM maestro_flow WHERE group_id=? AND flow_id=?";
  private static final String CLAIM_AND_RETURN_FLOWS_QUERY =
      "UPDATE maestro_flow SET generation=? WHERE group_id=? AND flow_id IN ("
          + "SELECT flow_id FROM maestro_flow WHERE group_id=? AND flow_id>? AND generation<? "
          + "ORDER BY flow_id ASC LIMIT ?) RETURNING flow_id,start_time,reference";
  private static final String HEARTBEAT_QUERY =
      "UPDATE maestro_flow_group SET heartbeat_ts=now() WHERE group_id=? AND generation=?";
  private static final String CLAIM_FLOW_GROUP_QUERY =
      "UPDATE maestro_flow_group SET (heartbeat_ts,generation,address)=(now(),generation+1,?) "
          + "WHERE group_id=(SELECT group_id FROM maestro_flow_group "
          + "WHERE heartbeat_ts<(now()-INTERVAL '1 second' * ?) FOR UPDATE SKIP LOCKED LIMIT 1)"
          + "RETURNING group_id,generation";
  private static final String ADD_FLOW_GROUP_QUERY =
      "INSERT INTO maestro_flow_group (group_id,generation,address) VALUES (?,?,?)";
  private static final String GET_FLOW_WITH_SAME_KEYS_QUERY =
      "SELECT 1 FROM maestro_flow WHERE group_id=? AND flow_id=? LIMIT 1";

  public MaestroFlowDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      FlowEngineProperties properties,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, properties, metrics);
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
    Checks.checkTrue(
        res == 1, "Insert flow row count for [%s] is not 1 but %s", flow.getReference(), res);
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

  /**
   * It heartbeats to keep the ownership of the flow group. It also detects if the ownership is lost
   * unexpectedly. It returns the flag (if the ownership is valid) to the caller.
   *
   * @param group flow group info
   * @return the flag to indicate if the group ownership is still valid
   */
  public boolean heartbeatGroup(FlowGroup group) {
    return 1
        == withMetricLogError(
            () ->
                withRetryableUpdate(
                    HEARTBEAT_QUERY,
                    stmt -> {
                      int idx = 0;
                      stmt.setLong(++idx, group.groupId());
                      stmt.setLong(++idx, group.generation());
                    }),
            "heartbeatGroup",
            "Failed to heartbeat the flow group for [{}]",
            group.groupId());
  }

  /**
   * Claim the expired ownership of a flow group. Note that it might claim the same expired group
   * again but will have a different generation.
   *
   * @param address owner's unique address
   * @param expiration expiration time to check
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
                    return new FlowGroup(result.getLong(1), result.getLong(2), address);
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
   * @param group new flow group to insert
   */
  public void insertGroup(FlowGroup group) {
    int res =
        withMetricLogError(
            () ->
                withRetryableUpdate(
                    ADD_FLOW_GROUP_QUERY,
                    stmt -> {
                      int idx = 0;
                      stmt.setLong(++idx, group.groupId());
                      stmt.setLong(++idx, group.generation());
                      stmt.setString(++idx, group.address());
                    }),
            "insertGroup",
            "Failed to insert the flow group for group id [{}]",
            group.groupId());
    Checks.checkTrue(
        res == 1, "Insert flow group row count for [%s] is not 1 but %s", group.groupId(), res);
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
}
