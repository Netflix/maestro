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

import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.models.api.StepOutputDataRequest;
import com.netflix.maestro.models.definition.StepType;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Workflow output data related REST API. It is called by the external job to output data from the
 * job execution back to Maestro step attempts.
 */
@Tag(name = "/api/v3", description = "Maestro Workflow Output Data APIs")
@Hidden
@RestController
@RequestMapping(
    value = "/api/v3",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
@Slf4j
public class OutputDataController {
  private static final String[] PLACEHOLDER_VALUE =
      new String[] {"PARAM_VALUE_IDENTICAL_TO_EVALUATED_RESULT"};
  private final OutputDataManager outputDataManager;

  @Autowired
  public OutputDataController(OutputDataManager outputDataManager) {
    this.outputDataManager = outputDataManager;
  }

  /** Upsert output data for step instance. Also verifies that app certificate matches payload. */
  @PutMapping(value = "/output-data", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Upsert a list of output data from an external job")
  public ResponseEntity<?> upsertOutputData(@RequestBody StepOutputDataRequest outputDataRequest) {
    LOG.debug("Upsert Output data: [{}]", outputDataRequest);
    OutputData outputData = deriveOutputDataWithIdentity(outputDataRequest);
    outputDataManager.saveOutputData(outputData);
    return ResponseEntity.ok().build();
  }

  /**
   * Handle workflow identity concerns. If identity provided in payload, verify against identity,
   * otherwise populate properties from identity.
   *
   * @param request output data request containing workflow identity
   */
  private OutputData deriveOutputDataWithIdentity(StepOutputDataRequest request) {
    // If the output data has a workflow identity, verify it against the expected identity.
    // For prod use cases, please overwrite this method to handle identity verification.
    // If invalid, should throw throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, ...).
    return OutputData.builder()
        .externalJobType(StepType.KUBERNETES)
        //        .externalJobId((String) request.getExtraInfo().get("external_job_type"))
        //        .externalJobId((String) request.getExtraInfo().get("external_job_id"))
        //        .externalJobId((String) request.getExtraInfo().get("workflow_id"))
        .params(request.getParams())
        .artifacts(request.getArtifacts())
        .build();
  }
}
