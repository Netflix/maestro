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

import com.netflix.maestro.engine.dao.OutputDataDao;
import com.netflix.maestro.engine.dto.ExternalJobType;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.models.artifact.Artifact;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

/** Manager class for handling Output Parameter validation and merging to parameter space. */
@AllArgsConstructor
public class OutputDataManager {

  private OutputDataDao outputDataDao;

  /** Merge back output parameters updated by step into params space along with validation. */
  public void validateAndMergeOutputParamsAndArtifacts(StepRuntimeSummary runtimeSummary) {
    Optional<String> externalJobId = extractExternalJobId(runtimeSummary);
    if (externalJobId.isPresent()) {
      Optional<OutputData> outputDataOpt =
          outputDataDao.getOutputDataForExternalJob(externalJobId.get(), ExternalJobType.TITUS);
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
    if (artifacts.containsKey(Artifact.Type.TITUS.key())) {
      String titusJobId = artifacts.get(Artifact.Type.TITUS.key()).asTitus().getTitusTaskId();
      if (titusJobId != null && !titusJobId.isEmpty()) {
        return Optional.of(titusJobId);
      }
    }
    return Optional.empty();
  }
}
