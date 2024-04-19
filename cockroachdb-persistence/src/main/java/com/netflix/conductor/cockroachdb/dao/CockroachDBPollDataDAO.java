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
import com.google.common.base.Preconditions;
import com.netflix.conductor.cockroachdb.CockroachDBConfiguration;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.dao.PollDataDAO;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CockroachDB implementation of PollDataDAO.
 *
 * @author jun-he
 */
public class CockroachDBPollDataDAO extends CockroachDBBaseDAO implements PollDataDAO {
  private static final Logger LOG = LoggerFactory.getLogger(CockroachDBPollDataDAO.class);
  private static final String DEFAULT_DOMAIN = "DEFAULT";

  private static final String UPSERT_POLL_DATA_STATEMENT =
      "UPSERT INTO poll_data (queue_name,domain,payload) VALUES (?,?,?)";
  private static final String GET_POLL_DATA_WITH_DOMAIN_STATEMENT =
      "SELECT payload FROM poll_data WHERE queue_name = ? AND domain = ?";
  private static final String GET_POLL_DATA_FOR_QUEUE_STATEMENT =
      "SELECT payload FROM poll_data WHERE queue_name = ?";

  public CockroachDBPollDataDAO(
      DataSource dataSource, ObjectMapper objectMapper, CockroachDBConfiguration config) {
    super(dataSource, objectMapper, config);
  }

  @Override
  public void updateLastPollData(String taskDefName, String domain, String workerId) {
    Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
    PollData pollData = new PollData(taskDefName, domain, workerId, System.currentTimeMillis());
    String actualDomain = (domain == null) ? DEFAULT_DOMAIN : domain;
    Integer cnt =
        withMetricLogError(
            () ->
                withRetryableUpdate(
                    UPSERT_POLL_DATA_STATEMENT,
                    statement -> {
                      statement.setString(1, pollData.getQueueName());
                      statement.setString(2, actualDomain);
                      statement.setString(3, toJson(pollData));
                    }),
            "updateLastPollData",
            "Failed updating last poll data {}",
            pollData);
    LOG.debug("Updated {} last poll data: {}", cnt, pollData);
  }

  @Override
  public PollData getPollData(String taskDefName, String domain) {
    Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
    String actualDomain = (domain == null) ? DEFAULT_DOMAIN : domain;
    return withMetricLogError(
        () ->
            getPayload(
                GET_POLL_DATA_WITH_DOMAIN_STATEMENT,
                statement -> {
                  statement.setString(1, taskDefName);
                  statement.setString(2, actualDomain);
                },
                PollData.class),
        "getPollData",
        "Failed getting last poll data with queue {} and domain {}",
        taskDefName,
        actualDomain);
  }

  @Override
  public List<PollData> getPollData(String taskDefName) {
    Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
    return withMetricLogError(
        () ->
            getPayloads(
                GET_POLL_DATA_FOR_QUEUE_STATEMENT,
                statement -> statement.setString(1, taskDefName),
                PollData.class),
        "getPollData",
        "Failed getting last poll data with queue {}",
        taskDefName);
  }
}
