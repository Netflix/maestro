/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.queue.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.utils.ObjectHelper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro internal queue dao implementation. It provides exactly-once publishing and at least once
 * consuming semantics. Note that owned_until has to be set on the server side using CLOCK_TIMESTAMP
 * to take care of the extremely long delay case (e.g., if the transaction takes 1 minute).
 */
@Slf4j
public class MaestroQueueDao extends AbstractDatabaseDao {

  private static final String ENQUEUE_MSG_QUERY =
      "INSERT INTO maestro_queue (queue_id,owned_until,msg_id,payload,create_time) VALUES (?,?,?,?,?)";
  private static final String POINT_QUERY_WHERE_CLAUSE =
      " WHERE queue_id=? AND owned_until=? AND msg_id=?";
  private static final String REMOVE_MSG_QUERY =
      "DELETE FROM maestro_queue" + POINT_QUERY_WHERE_CLAUSE;
  private static final String EXTEND_MSG_OWNERSHIP_QUERY =
      "UPDATE maestro_queue SET owned_until=?" + POINT_QUERY_WHERE_CLAUSE;
  private static final String REPLACE_MSG_QUERY =
      "UPDATE maestro_queue SET (queue_id,owned_until,msg_id,payload,create_time)=(?,?,?,?,?)"
          + POINT_QUERY_WHERE_CLAUSE;

  private static final String ADD_LOCK_IF_ABSENT_QUERY =
      "INSERT INTO maestro_queue_lock (queue_id) VALUES (?) ON CONFLICT DO NOTHING";
  private static final String LOCK_FOR_SCAN_QUERY =
      "SELECT queue_id FROM maestro_queue_lock WHERE queue_id=? FOR UPDATE SKIP LOCKED";

  private static final String DEQUEUE_UNOWNED_MSGS_QUERY =
      "UPDATE maestro_queue SET owned_until=(EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())*1000)::INT8 + ? "
          + "WHERE queue_id=? AND (owned_until,msg_id) IN (SELECT owned_until,msg_id FROM maestro_queue "
          + "WHERE queue_id=? AND owned_until<(EXTRACT(EPOCH FROM NOW())::INT8*1000) ORDER BY owned_until ASC "
          + "LIMIT ?) RETURNING owned_until,msg_id,payload,create_time";
  private static final String RELEASE_MSGS_QUERY_TEMPLATE =
      "UPDATE maestro_queue SET owned_until=? WHERE queue_id=? AND (owned_until,msg_id) IN (%s)";
  private static final String VALUE_PLACE_HOLDER = "(?,?)";

  public MaestroQueueDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
  }

  /**
   * Insert the message into the queue table. It only ensures the message is inserted once within
   * the same transaction. It's caller's responsibility to ensure the same message is added only
   * once. The queue table itself does not provide a deduplication key. For example, if the caller
   * invokes delete workflow transaction happens twice for the same workflow, then there will be two
   * messages in the queue. The caller should not use the queue as a mechanism for the duplication.
   *
   * @param conn db connection
   * @param jobEvent message payload to insert
   * @param validUntil the timestamp before losing the ownership of queued message in this worker.
   * @return the added message, if null, it means not need to notify
   * @throws SQLException if the db operation fails or conflicts
   */
  public MessageDto enqueueInTransaction(Connection conn, MaestroJobEvent jobEvent, long validUntil)
      throws SQLException {
    String msgId = jobEvent.deriveMessageKey();
    long curTime = System.currentTimeMillis();
    try (PreparedStatement stmt = conn.prepareStatement(ENQUEUE_MSG_QUERY)) {
      int idx = 0;
      stmt.setInt(++idx, jobEvent.getType().getQueueId());
      stmt.setLong(++idx, validUntil);
      stmt.setString(++idx, msgId);
      stmt.setString(++idx, toJson(jobEvent));
      stmt.setLong(++idx, curTime);
      if (stmt.executeUpdate() == SUCCESS_WRITE_SIZE) {
        return new MessageDto(validUntil, msgId, jobEvent, curTime);
      }
      return null;
    }
  }

  /**
   * Insert the message into the queue table. The queue table itself does not provide a
   * deduplication key. The caller should not use the queue as a mechanism for the duplication.
   *
   * @param jobEvent message payload to insert
   * @param validUntil the timestamp before losing the ownership of queued message in this worker.
   * @return the added message
   */
  public MessageDto enqueue(MaestroJobEvent jobEvent, long validUntil) {
    String msgId = jobEvent.deriveMessageKey();
    String jobStr = toJson(jobEvent);
    long curTime = System.currentTimeMillis();
    return withMetricLogError(
        () -> {
          int res =
              withRetryableUpdate(
                  ENQUEUE_MSG_QUERY,
                  stmt -> {
                    int idx = 0;
                    stmt.setInt(++idx, jobEvent.getType().getQueueId());
                    stmt.setLong(++idx, validUntil);
                    stmt.setString(++idx, msgId);
                    stmt.setString(++idx, jobStr);
                    stmt.setLong(++idx, curTime);
                  });
          if (res == SUCCESS_WRITE_SIZE) {
            return new MessageDto(validUntil, msgId, jobEvent, curTime);
          }
          return null;
        },
        "enqueue",
        "Failed to enqueue the job event [{}]",
        jobEvent);
  }

  /**
   * Delete the message after it is completed.
   *
   * @param message message to delete
   */
  public void remove(MessageDto message) {
    withMetricLogError(
        () ->
            withRetryableUpdate(
                REMOVE_MSG_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setInt(++idx, message.queueId());
                  stmt.setLong(++idx, message.ownedUntil());
                  stmt.setString(++idx, message.msgId());
                }),
        "remove",
        "Failed to delete the message [{}]",
        message);
  }

  /**
   * Best effort to extend the ownership of the message. If the message is not owned by this worker,
   * it returns null. If failed to extend the ownership, it returns null too.
   *
   * @param message the message to extend
   * @param validUntil the timestamp before losing the ownership of queued message in this worker.
   * @return the message with extended ownership, or null if failed
   */
  public MessageDto extendOwnership(MessageDto message, long validUntil) {
    try {
      int res =
          withRetryableUpdate(
              EXTEND_MSG_OWNERSHIP_QUERY,
              stmt -> {
                int idx = 0;
                stmt.setLong(++idx, validUntil);
                stmt.setInt(++idx, message.queueId());
                stmt.setLong(++idx, message.ownedUntil());
                stmt.setString(++idx, message.msgId());
              });
      if (res == SUCCESS_WRITE_SIZE) {
        var msg =
            new MessageDto(validUntil, message.msgId(), message.event(), message.createTime());
        LOG.debug("Extended ownership of message to [{}]", msg);
        return msg;
      } else {
        LOG.warn(
            "Extending ownership of message [{}] returns unexpected result [{}]", message, res);
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed extending the message [{}][{}] ownership and will let it expire due to",
          message.queueId(),
          message.msgId(),
          e);
    }
    return null;
  }

  /**
   * Insert the message into the queue table to replace an existing message.
   *
   * @param message the current message to be replaced
   * @param jobEvent the new message payload to replace
   * @param validUntil the timestamp before losing the ownership of queued message in this worker.
   * @return the added message
   */
  public MessageDto replace(MessageDto message, MaestroJobEvent jobEvent, long validUntil) {
    String msgId = jobEvent.deriveMessageKey();
    String jobStr = toJson(jobEvent);
    long curTime = System.currentTimeMillis();
    return withMetricLogError(
        () -> {
          int res =
              withRetryableUpdate(
                  REPLACE_MSG_QUERY,
                  stmt -> {
                    int idx = 0;
                    stmt.setInt(++idx, jobEvent.getType().getQueueId());
                    stmt.setLong(++idx, validUntil);
                    stmt.setString(++idx, msgId);
                    stmt.setString(++idx, jobStr);
                    stmt.setLong(++idx, curTime);
                    stmt.setInt(++idx, message.queueId());
                    stmt.setLong(++idx, message.ownedUntil());
                    stmt.setString(++idx, message.msgId());
                  });
          if (res == SUCCESS_WRITE_SIZE) {
            return new MessageDto(validUntil, msgId, jobEvent, curTime);
          }
          return null;
        },
        "replace",
        "Failed to replace the job event from [{}] to [{}]",
        message.event(),
        jobEvent);
  }

  private record Message(long ownedUntil, String id, String payload, long createTime) {}

  /**
   * Retry the un-owned message after the current ownership is timed out. It will put a lock in the
   * queue_lock table to ensure that only one worker can dequeue the un-owned for a given queue_id.
   *
   * @return the list of owned messages to process
   */
  public List<MessageDto> dequeueUnownedMessages(int queueId, long expirationTimeout, int limit) {
    List<Message> msgs =
        withMetricLogError(
            () ->
                withRetryableTransaction(
                    conn -> {
                      try (PreparedStatement lockStmt =
                          conn.prepareStatement(LOCK_FOR_SCAN_QUERY)) {
                        lockStmt.setInt(1, queueId);
                        try (ResultSet result = lockStmt.executeQuery()) {
                          if (result.next()) { // if the lock for the queue_id is obtained
                            try (PreparedStatement dequeueStmt =
                                conn.prepareStatement(DEQUEUE_UNOWNED_MSGS_QUERY)) {
                              int idx = 0;
                              dequeueStmt.setLong(++idx, expirationTimeout);
                              dequeueStmt.setInt(++idx, queueId);
                              dequeueStmt.setInt(++idx, queueId);
                              dequeueStmt.setInt(++idx, limit);
                              try (ResultSet res = dequeueStmt.executeQuery()) {
                                List<Message> messages = new ArrayList<>();
                                while (res.next()) {
                                  idx = 0;
                                  messages.add(
                                      new Message(
                                          res.getLong(++idx),
                                          res.getString(++idx),
                                          res.getString(++idx),
                                          res.getLong(++idx)));
                                }
                                return messages;
                              }
                            }
                          } else {
                            return Collections.emptyList();
                          }
                        }
                      }
                    }),
            "dequeueUnownedMessages",
            "Failed to dequeueUnownedMessages for queue_id [{}] with limit [{}]",
            queueId,
            limit);
    return msgs.stream()
        .map(
            msg ->
                new MessageDto(
                    msg.ownedUntil,
                    msg.id,
                    fromJson(msg.payload, MaestroJobEvent.class),
                    msg.createTime))
        .toList();
  }

  /**
   * Add a lock for the given queue id if not exists. It is idempotent.
   *
   * @param queueId queue id
   * @return the number of rows affected (0 or 1)
   */
  public int addLockIfAbsent(int queueId) {
    return withMetricLogError(
        () -> withRetryableUpdate(ADD_LOCK_IF_ABSENT_QUERY, stmt -> stmt.setInt(1, queueId)),
        "addLockIfAbsent",
        "Failed to add lock to the maestro_queue_lock for queue_id [{}]",
        queueId);
  }

  /**
   * Release the owned messages in the queue with the best effort. If failed, it will let the
   * message expire.
   *
   * @param queueId queue id
   * @param validUntil the timestamp before losing the ownership of queued message in this worker.
   * @param messages the messages to release
   * @return the error details if failed, otherwise empty
   */
  public Optional<Details> release(int queueId, long validUntil, List<MessageDto> messages) {
    if (ObjectHelper.isCollectionEmptyOrNull(messages)) {
      return Optional.empty();
    }
    try {
      String sql =
          String.format(
              RELEASE_MSGS_QUERY_TEMPLATE,
              String.join(",", Collections.nCopies(messages.size(), VALUE_PLACE_HOLDER)));
      int res =
          withRetryableUpdate(
              sql,
              stmt -> {
                int idx = 0;
                stmt.setLong(++idx, validUntil);
                stmt.setInt(++idx, queueId);
                for (MessageDto message : messages) {
                  stmt.setLong(++idx, message.ownedUntil());
                  stmt.setString(++idx, message.msgId());
                }
              });
      LOG.info("released [{}] messages for queue_id [{}]", res, queueId);
      return Optional.empty();
    } catch (RuntimeException e) {
      return Optional.of(
          Details.create(
              e,
              false,
              String.format(
                  "Failed releasing the messages for queue_id [%s] and will let them expire.",
                  queueId)));
    }
  }
}
