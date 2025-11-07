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
package com.netflix.maestro.engine.params;

import com.netflix.maestro.engine.dao.MaestroOutputDataDao;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringArrayParameter;
import com.netflix.maestro.models.parameter.StringParameter;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.AllArgsConstructor;

/** Manager class for handling Output Parameter validation and merging to parameter space. */
@AllArgsConstructor
public class OutputDataManager {
  private static final String PLACEHOLDER_STRING_VALUE =
      "PARAM_VALUE_IDENTICAL_TO_EVALUATED_RESULT";
  private static final String[] PLACEHOLDER_ARRAY_VALUE = new String[] {PLACEHOLDER_STRING_VALUE};
  private static final int PLACEHOLDER_THRESHOLD = 1000;

  private MaestroOutputDataDao outputDataDao;

  /**
   * Save output data to database. It also replaces values with placeholders in following cases:
   * <li>- replace values with placeholders when identical to evaluated results for string array
   *     params
   * <li>- replace values longer than 1K threshold with placeholders when identical to evaluated
   *     results for string params
   *
   * @param outputData output data to save
   */
  public void saveOutputData(OutputData outputData) {
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
                    entry.setValue(sp.toBuilder().value(PLACEHOLDER_ARRAY_VALUE).build());
                  }
                } else if (p.getType() == ParamType.STRING) {
                  StringParameter sp = p.asStringParam();
                  if (sp.getEvaluatedResult().length() > PLACEHOLDER_THRESHOLD
                      && Objects.equals(sp.getEvaluatedResult(), sp.getValue())) {
                    entry.setValue(sp.toBuilder().value(PLACEHOLDER_STRING_VALUE).build());
                  }
                }
              });
    }
    outputDataDao.insertOrUpdateOutputData(outputData);
  }

  /** Merge back output parameters updated by step into params space along with validation. */
  public void validateAndMergeOutputParamsAndArtifacts(StepRuntimeSummary runtimeSummary) {
    Optional<String> externalJobId = extractExternalJobId(runtimeSummary);
    if (externalJobId.isPresent()) {
      Optional<OutputData> outputDataOpt =
          outputDataDao.getOutputDataForExternalJob(externalJobId.get(), runtimeSummary.getType());
      outputDataOpt.ifPresent(
          outputData -> {
            // merge output params if any.
            if (outputData.getParams() != null) {
              ParamsMergeHelper.mergeOutputDataParams(
                  runtimeSummary.getParams(), outputData.getParams());
            }
            // merge output artifacts if any.
            if (outputData.getArtifacts() != null) {
              runtimeSummary.mergeRuntimeUpdate(null, outputData.getArtifacts());
            }
          });
    }
  }

  private Optional<String> extractExternalJobId(StepRuntimeSummary runtimeSummary) {
    Map<String, Artifact> artifacts = runtimeSummary.getArtifacts();
    String jobId = null;
    if (artifacts.containsKey(Artifact.Type.KUBERNETES.key())) {
      jobId = artifacts.get(Artifact.Type.KUBERNETES.key()).asKubernetes().getJobId();
    } else if (artifacts.containsKey(Artifact.Type.TITUS.key())) {
      jobId = artifacts.get(Artifact.Type.TITUS.key()).asTitus().getTitusTaskId();
    } else if (artifacts.containsKey(Artifact.Type.HTTP.key())) {
      jobId = runtimeSummary.getStepInstanceUuid();
    }
    if (jobId != null && !jobId.isEmpty()) {
      return Optional.of(jobId);
    } else {
      return Optional.empty();
    }
  }
}
