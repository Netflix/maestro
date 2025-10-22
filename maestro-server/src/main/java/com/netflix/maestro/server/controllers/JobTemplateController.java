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
package com.netflix.maestro.server.controllers;

import com.netflix.maestro.engine.dao.MaestroJobTemplateDao;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.api.JobTemplateCreateRequest;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Controller class for job template schema operations. */
@Tag(name = "/api/v3/job-templates", description = "Maestro JobTemplate APIs")
@RestController
@RequestMapping(
    value = "/api/v3/job-templates",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class JobTemplateController {
  private final MaestroJobTemplateDao jobTemplateDao;
  private final User.UserBuilder callerBuilder;

  /** Constructor. */
  @Autowired
  public JobTemplateController(
      MaestroJobTemplateDao jobTemplateDao, User.UserBuilder callerBuilder) {
    this.jobTemplateDao = jobTemplateDao;
    this.callerBuilder = callerBuilder;
  }

  /**
   * Create or Update a job template.
   *
   * @param createRequest job template create request
   */
  @PostMapping(value = "")
  @Operation(summary = "Create or update a job template.")
  public JobTemplate upsertJobTemplate(@Valid @RequestBody JobTemplateCreateRequest createRequest) {
    JobTemplate jobTemplate = new JobTemplate();
    var metadata = new JobTemplate.Metadata();
    metadata.setOwner(createRequest.getOwner());
    metadata.setVersionAuthor(callerBuilder.build());
    metadata.setStatus(createRequest.getStatus());
    metadata.setSupport(createRequest.getSupport());
    metadata.setTestWorkflows(createRequest.getTestWorkflows());
    metadata.setGitInfo(createRequest.getGitInfo());
    metadata.setCreateTime(System.currentTimeMillis());
    metadata.setExtraInfo(createRequest.getExtraInfo());
    jobTemplate.setMetadata(metadata);
    jobTemplate.setDefinition(createRequest.getDefinition());
    jobTemplateDao.upsertJobTemplate(jobTemplate);
    return jobTemplate;
  }

  /**
   * Create or Update a job template in yaml format.
   *
   * @param createRequest job template create request
   */
  @PostMapping(value = "/yaml", consumes = MediaType.APPLICATION_YAML_VALUE)
  @Operation(summary = "Create or update a job template.")
  public JobTemplate upsertJobTemplateYaml(
      @Valid @RequestBody JobTemplateCreateRequest createRequest) {
    return upsertJobTemplate(createRequest);
  }

  /**
   * Fetch an existing job template definition.
   *
   * @param jobType job type
   * @param version version value, e.g. "default"
   * @return the job template
   */
  @GetMapping(value = "/{type}", consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Fetches an existing job template type.")
  public JobTemplate getJobTemplate(
      @Valid @NotNull @PathVariable("type") String jobType,
      @RequestParam(value = "version", defaultValue = "default") String version) {
    var jobTemplate = jobTemplateDao.getJobTemplate(jobType, version);
    if (jobTemplate == null) {
      throw new MaestroNotFoundException(
          "No job template found for job_type [%s] and version [%s]", jobType, version);
    }
    return jobTemplate;
  }

  /**
   * Remove an existing job type.
   *
   * @param jobType job type
   * @param version version value, e.g. "default"
   */
  @DeleteMapping(value = "/{type}", consumes = MediaType.ALL_VALUE)
  @Operation(
      summary = "Removes an existing job template type, if version is null, delete all versions.")
  public ResponseEntity<?> removeJobTemplate(
      @Valid @NotNull @PathVariable("type") String jobType,
      @RequestParam(value = "version", required = false) String version) {
    int cnt = jobTemplateDao.removeJobTemplate(jobType, version);
    return ResponseEntity.ok()
        .body(
            String.format(
                "Removed [%s] job template versions for job type: [%s][%s]",
                cnt, jobType, version));
  }
}
