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
package com.netflix.maestro.engine.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.stepruntime.KubernetesCommand;
import com.netflix.maestro.utils.Checks;
import java.util.Collections;
import java.util.Locale;
import lombok.AllArgsConstructor;

/** Generate Kubernetes command for workflow step. Extract data from parameters. */
@AllArgsConstructor
public class KubernetesCommandGenerator {
  private static final String KUBERNETES_KEY = StepType.KUBERNETES.getType().toLowerCase(Locale.US);
  private static final String IMAGE_KEY = "image";
  private static final String ENTRYPOINT_KEY = "entrypoint";

  private final ObjectMapper mapper;

  /**
   * Generate Kubernetes command for workflow step. Extract data from parameters.
   *
   * @param context Kubernetes step context
   * @return a Kubernetes command object
   */
  public KubernetesCommand generate(KubernetesStepContext context) {
    MapParameter mapParams = getAndValidateKubernetesParams(context.getRuntimeSummary());
    KubernetesCommand command =
        mapper.convertValue(mapParams.getEvaluatedResult(), KubernetesCommand.class);
    var builder = command.toBuilder();
    if (command.getJobDeduplicationKey() == null) {
      builder.jobDeduplicationKey(context.getRuntimeSummary().getStepInstanceUuid());
    }
    if (command.getAppName() == null) {
      builder.appName(Constants.MAESTRO_QUALIFIER);
    }
    if (command.getEnv() == null) {
      builder.env(Collections.emptyMap());
    }
    return builder.build();
  }

  /**
   * Get and validate Kubernetes parameters.
   *
   * @param stepSummary step runtime summary
   * @return Kubernetes parameters
   */
  private MapParameter getAndValidateKubernetesParams(StepRuntimeSummary stepSummary) {
    Checks.notNull(stepSummary.getParams(), "params must be present");
    Checks.notNull(
        stepSummary.getParams().get(KUBERNETES_KEY), "kubernetes params must be present");
    MapParameter mapParams = stepSummary.getParams().get(KUBERNETES_KEY).asMapParam();
    checkNotNullPrecondition(mapParams, IMAGE_KEY);
    checkNotNullPrecondition(mapParams, ENTRYPOINT_KEY);
    return mapParams;
  }

  private void checkNotNullPrecondition(MapParameter mapParams, String key) {
    Checks.notNull(mapParams.getEvaluatedParam(key), key + " cannot be null");
  }
}
