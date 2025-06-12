package com.netflix.maestro.server.controllers;

import com.netflix.maestro.engine.dao.OutputDataDao;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.models.api.StepOutputDataRequest;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringArrayParameter;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Arrays;
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
  private final OutputDataDao outputDataDao;

  @Autowired
  public OutputDataController(OutputDataDao outputDataDao) {
    this.outputDataDao = outputDataDao;
  }

  /** Upsert output data for step instance. Also verifies that app certificate matches payload. */
  @PutMapping(value = "/output-data", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Upsert a list of output data from an external job")
  public ResponseEntity<?> upsertOutputData(@RequestBody StepOutputDataRequest outputDataRequest) {
    LOG.debug("Upsert Output data: [{}]", outputDataRequest);
    OutputData outputData = deriveOutputDataWithIdentity(outputDataRequest);
    // removing value in the output string array param if it is identical to evaluated result
    if (outputData.getParams() != null) {
      outputData
          .getParams()
          .entrySet()
          .forEach(
              entry -> {
                Parameter p = entry.getValue();
                if (p.getType() == ParamType.STRING_ARRAY) {
                  StringArrayParameter sp = p.asStringArrayParam();
                  if (Arrays.equals(sp.getEvaluatedResult(), sp.getValue())) {
                    entry.setValue(sp.toBuilder().value(PLACEHOLDER_VALUE).build());
                  }
                }
              });
    }
    outputDataDao.insertOrUpdateOutputData(outputData);
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
