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
package com.netflix.maestro.engine.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.cockroachdb.CockroachDBConfiguration;
import com.netflix.conductor.cockroachdb.dao.CockroachDBBaseDAO;
import com.netflix.maestro.engine.dto.ExternalJobType;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.utils.Checks;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;

/** DAO for saving and retrieving output data. */
public class OutputDataDao extends CockroachDBBaseDAO {
  private static final String GET_OUTPUT_DATA_JOB_QUERY =
      "SELECT payload, create_ts, modify_ts from output_data "
          + "WHERE external_job_id = ? AND external_job_type = ? LIMIT 1";
  private static final String UPSERT_OUTPUT_DATA_QUERY =
      "UPSERT INTO output_data " + "(payload, modify_ts)" + " VALUES " + "(?, CURRENT_TIMESTAMP)";

  /** Constructor for OutputDataDAO. */
  public OutputDataDao(
      DataSource dataSource, ObjectMapper objectMapper, CockroachDBConfiguration config) {
    super(dataSource, objectMapper, config);
  }

  /**
   * Return output data {@link OutputData} for specific step instance.
   *
   * @param externalJobId External Job ID such as Titus Instance ID
   * @param externalJobType ExternalJobType {@link ExternalJobType}
   * @return optional output data
   */
  public Optional<OutputData> getOutputDataForExternalJob(
      String externalJobId, ExternalJobType externalJobType) {
    return withRetryableQuery(
        GET_OUTPUT_DATA_JOB_QUERY,
        stmt -> {
          stmt.setString(1, externalJobId);
          stmt.setString(2, externalJobType.toString());
        },
        this::outputDataFromResult);
  }

  /**
   * Upsert output data object.
   *
   * @param outputData output data
   */
  public void insertOrUpdateOutputData(OutputData outputData) {
    validate(outputData);
    withMetricLogError(
        () ->
            withRetryableUpdate(
                UPSERT_OUTPUT_DATA_QUERY, stmt -> stmt.setString(1, toJson(outputData))),
        "insertOrUpdateOutputParameters",
        "Failed updating output data {}",
        outputData);
  }

  private Optional<OutputData> outputDataFromResult(ResultSet rs) throws SQLException {
    if (rs.next()) {
      OutputData data = fromJson(rs.getString("payload"), OutputData.class);
      data.setCreateTime(rs.getTimestamp("create_ts").getTime());
      data.setModifyTime(rs.getTimestamp("modify_ts").getTime());
      return Optional.of(data);
    } else {
      return Optional.empty();
    }
  }

  private void validate(OutputData data) {
    Checks.notNull(data, "data object cannot be null");
    Checks.notNull(data.getExternalJobId(), "External job id cannot be null");
    Checks.notNull(data.getExternalJobType(), "External Job Type cannot be null");
    Checks.notNull(data.getWorkflowId(), "Workflow id cannot be null");
    Checks.notNull(data.getParams(), "Output data cannot be null");
    Checks.checkTrue(!data.getParams().isEmpty(), "Output data cannot be empty");
  }
}
