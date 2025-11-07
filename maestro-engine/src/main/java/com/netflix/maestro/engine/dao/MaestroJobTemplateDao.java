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
package com.netflix.maestro.engine.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/** DAO for saving and retrieving maestro job template schema data. */
public class MaestroJobTemplateDao extends AbstractDatabaseDao {
  private static final String GET_JOB_TEMPLATE_DEFINITION_QUERY =
      "SELECT metadata, definition from maestro_job_template WHERE job_type=? AND version=?";
  private static final String DELETE_A_JOB_TEMPLATE_VERSION_QUERY =
      "DELETE FROM maestro_job_template WHERE job_type=? AND version=?";
  private static final String DELETE_ALL_JOB_TEMPLATE_VERSIONS_QUERY =
      "DELETE FROM maestro_job_template WHERE job_type=?";

  private static final String UPSERT_JOB_TEMPLATE_QUERY =
      "INSERT INTO maestro_job_template (job_type,version,metadata,definition) "
          + "VALUES (?, ?, ?::json, ?::json) ON CONFLICT (job_type, version) "
          + "DO UPDATE SET metadata = EXCLUDED.metadata, definition = EXCLUDED.definition";

  private static final String GET_ALL_JOB_TEMPLATE_DEFINITIONS_QUERY =
      "SELECT metadata, definition from maestro_job_template WHERE version=?";

  /** Constructor for MaestroJobTemplateDao. */
  public MaestroJobTemplateDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
  }

  /**
   * Return job template schema {@link JobTemplate} for a job type and its version.
   *
   * @param jobType job template id, which is always same as step sub_type
   * @param version job template version value, e.g. "default"
   * @return step schema or null if not found
   */
  public JobTemplate getJobTemplate(String jobType, String version) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_JOB_TEMPLATE_DEFINITION_QUERY,
                stmt -> {
                  stmt.setString(1, jobType);
                  stmt.setString(2, version);
                },
                result -> {
                  if (result.next()) {
                    var ret = new JobTemplate();
                    ret.setMetadata(fromJson(result.getString(1), JobTemplate.Metadata.class));
                    ret.setDefinition(fromJson(result.getString(2), JobTemplate.Definition.class));
                    return ret;
                  }
                  return null;
                }),
        "getJobTemplate",
        "Failed get the job template schema for [{}][{}]",
        jobType,
        version);
  }

  /**
   * Remove job template(s) by job type and optional version.
   *
   * @param jobType the job type to remove
   * @param version the version to remove, or null to remove all versions for this job type
   * @return the number of job templates removed
   */
  public int removeJobTemplate(String jobType, @Nullable String version) {
    String deleteQuery =
        version != null
            ? DELETE_A_JOB_TEMPLATE_VERSION_QUERY
            : DELETE_ALL_JOB_TEMPLATE_VERSIONS_QUERY;
    return withMetricLogError(
        () ->
            withRetryableUpdate(
                deleteQuery,
                stmt -> {
                  stmt.setString(1, jobType);
                  if (version != null) {
                    stmt.setString(2, version);
                  }
                }),
        "removeJobTemplate",
        "Failed to remove job template for [{}][{}]",
        jobType,
        version);
  }

  /**
   * Insert or update a job template.
   *
   * @param jobTemplate the job template to upsert
   */
  public void upsertJobTemplate(JobTemplate jobTemplate) {
    var def = jobTemplate.getDefinition();
    withMetricLogError(
        () ->
            withRetryableUpdate(
                UPSERT_JOB_TEMPLATE_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, def.getJobType());
                  stmt.setString(++idx, def.getVersion());
                  stmt.setString(++idx, toJson(jobTemplate.getMetadata()));
                  stmt.setString(++idx, toJson(def));
                }),
        "upsertJobTemplate",
        "Failed to upsert job template for [{}][{}]",
        def.getJobType(),
        def.getVersion());
  }

  /**
   * Return all job template schemas {@link JobTemplate} for a version.
   *
   * @param version job template version value, e.g. "default"
   * @return all job templates for a given version
   */
  @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
  public List<JobTemplate> getJobTemplates(String version) {
    List<JobTemplate> jobTemplates = new ArrayList<>();
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_ALL_JOB_TEMPLATE_DEFINITIONS_QUERY,
                stmt -> stmt.setString(1, version),
                result -> {
                  while (result.next()) {
                    var res = new JobTemplate();
                    res.setMetadata(fromJson(result.getString(1), JobTemplate.Metadata.class));
                    res.setDefinition(fromJson(result.getString(2), JobTemplate.Definition.class));
                    jobTemplates.add(res);
                  }
                  return jobTemplates;
                }),
        "getJobTemplates",
        "Failed get all the job templates for version [{}]",
        version);
  }
}
