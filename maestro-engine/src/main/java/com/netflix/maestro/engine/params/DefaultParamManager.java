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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** DefaultParamManager used to manage various levels of default and schema level parameters. */
@Slf4j
public class DefaultParamManager {
  private static final String WORKFLOW_PARAMS_FILE = "defaultparams/default-workflow-params.yaml";
  private static final String NETFLIX_PARAMS_FILE = "defaultparams/default-step-params.yaml";
  private static final String STEP_TYPE_PARAMS_FILE = "defaultparams/default-%s-step-params.yaml";
  private static final String DRY_RUN_PARAMS_FILE = "defaultparams/default-dry-run-params.yaml";

  private final ObjectMapper objectMapper;
  private final TypeReference<Map<String, ParamDefinition>> typeRef = new TypeReference<>() {};
  private Map<String, ParamDefinition> defaultSystemParams;
  private Map<String, ParamDefinition> defaultStepParams;
  private Map<String, Map<String, ParamDefinition>> defaultTypeParams;
  private Map<String, ParamDefinition> defaultDryRunParams;

  /**
   * Constructor.
   *
   * @param objectMapper object mapper
   */
  public DefaultParamManager(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /** Postconstruct initialization for DefaultParamManager. */
  public void init() throws IOException {
    defaultSystemParams = loadParamsFromFile(WORKFLOW_PARAMS_FILE);
    defaultStepParams = loadParamsFromFile(NETFLIX_PARAMS_FILE);
    defaultDryRunParams = loadParamsFromFile(DRY_RUN_PARAMS_FILE);
    defaultTypeParams = new HashMap<>();
    for (StepType stepType : StepType.values()) {
      String stepName = stepType.toString().toLowerCase(Locale.US);
      String defaultFile = String.format(STEP_TYPE_PARAMS_FILE, stepName);
      try {
        URL filename = Thread.currentThread().getContextClassLoader().getResource(defaultFile);
        if (filename != null) {
          LOG.info("Loading default param file for {} step", stepName);
          defaultTypeParams.put(stepName, loadParamsFromFile(defaultFile));
        }
      } catch (Exception e) {
        throw new MaestroRuntimeException("Error processing step default file " + defaultFile, e);
      }
    }
  }

  /**
   * Return default workflow params to be injected.
   *
   * @return default workflow params
   */
  public Map<String, ParamDefinition> getDefaultWorkflowParams() {
    return preprocessParams(defaultSystemParams);
  }

  /**
   * Return default parameters to be injected.
   *
   * @return default step params
   */
  public Map<String, ParamDefinition> getDefaultStepParams() {
    return preprocessParams(defaultStepParams);
  }

  /**
   * Return dry run parameter defaults.
   *
   * @return default dry run params
   */
  public Map<String, ParamDefinition> getDefaultDryRunParams() {
    return preprocessParams(defaultDryRunParams);
  }

  /**
   * Get default params for Step type.
   *
   * @param stepType step type
   * @return optional type to default param map
   */
  public Optional<Map<String, ParamDefinition>> getDefaultParamsForType(StepType stepType) {
    Map<String, ParamDefinition> defaults =
        defaultTypeParams.get(stepType.toString().toLowerCase(Locale.US));
    if (defaults != null) {
      return Optional.of(preprocessParams(defaults));
    } else {
      return Optional.empty();
    }
  }

  /** Load params from JSON file. */
  private Map<String, ParamDefinition> loadParamsFromFile(String paramsFile) throws IOException {
    return objectMapper.readValue(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(paramsFile), typeRef);
  }

  private Map<String, ParamDefinition> preprocessParams(Map<String, ParamDefinition> params) {
    if (params == null) {
      return null;
    }
    Map<String, ParamDefinition> result = objectMapper.convertValue(params, typeRef);
    result.forEach((n, p) -> p.setName(n));
    return result;
  }
}
