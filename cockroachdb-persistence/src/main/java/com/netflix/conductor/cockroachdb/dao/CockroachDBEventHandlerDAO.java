/*
 * Copyright 2024 Netflix, Inc.
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
package com.netflix.conductor.cockroachdb.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.cockroachdb.CockroachDBConfiguration;
import com.netflix.conductor.cockroachdb.util.StatementPreparer;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.dao.EventHandlerDAO;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CockroachDB implementation of EventHandlerDAO.
 *
 * <p>It manages event handler definition metadata.
 *
 * @author jun-he
 */
public class CockroachDBEventHandlerDAO extends CockroachDBBaseDAO implements EventHandlerDAO {
  private static final Logger LOG = LoggerFactory.getLogger(CockroachDBEventHandlerDAO.class);

  private static final String UPSERT_EVENT_HANDLER_STATEMENT =
      "UPSERT INTO event_handler (handler_name,payload) VALUES (?,?)";
  private static final String REMOVE_EVENT_HANDLER_STATEMENT =
      "DELETE FROM event_handler WHERE handler_name = ?";
  private static final String GET_EVENT_HANDLERS_STATEMENT = "SELECT payload FROM event_handler";
  private static final String GET_EVENT_HANDLERS_FOR_EVENT_STATEMENT =
      "SELECT payload FROM event_handler where event = ? and active = ?";

  public CockroachDBEventHandlerDAO(
      DataSource dataSource, ObjectMapper objectMapper, CockroachDBConfiguration config) {
    super(dataSource, objectMapper, config);
  }

  @Override
  public void addEventHandler(EventHandler eventHandler) {
    LOG.info("Creating an event handler with name: {}", eventHandler.getName());
    upsertEventHandler(eventHandler, "addEventHandler");
  }

  @Override
  public void updateEventHandler(EventHandler eventHandler) {
    LOG.info("Updating an event handler with name: {}", eventHandler.getName());
    upsertEventHandler(eventHandler, "updateEventHandler");
  }

  private void upsertEventHandler(EventHandler eventHandler, String methodName) {
    withMetricLogError(
        () ->
            withRetryableUpdate(
                UPSERT_EVENT_HANDLER_STATEMENT,
                statement -> {
                  statement.setString(1, eventHandler.getName());
                  statement.setString(2, toJson(eventHandler));
                }),
        methodName,
        "Failed {} for handler: {}",
        methodName,
        eventHandler);
  }

  @Override
  public void removeEventHandler(String name) {
    withMetricLogError(
        () ->
            withRetryableUpdate(
                REMOVE_EVENT_HANDLER_STATEMENT, statement -> statement.setString(1, name)),
        "removeEventHandler",
        "Failed removing an event handler with name {}",
        name);
  }

  @Override
  public List<EventHandler> getAllEventHandlers() {
    return withMetricLogError(
        () ->
            getPayloads(GET_EVENT_HANDLERS_STATEMENT, StatementPreparer.NO_OP, EventHandler.class),
        "getAllEventHandlers",
        "Failed getting all event handlers");
  }

  @Override
  public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
    return withMetricLogError(
        () ->
            getPayloads(
                GET_EVENT_HANDLERS_FOR_EVENT_STATEMENT,
                statement -> {
                  statement.setString(1, event);
                  statement.setBoolean(2, activeOnly);
                },
                EventHandler.class),
        "getEventHandlersForEvent",
        "Failed getting all event handlers for [active {}] event {}",
        activeOnly,
        event);
  }
}
