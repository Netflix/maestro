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
package com.netflix.maestro.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.maestro.exceptions.MaestroInternalError;

/** Json helper utility class. */
public final class JsonHelper {
  private JsonHelper() {}

  /** Create Jackson object mapper with maestro needed features. */
  public static ObjectMapper objectMapper() {
    final ObjectMapper mapper = new ObjectMapper();
    configureMapper(mapper);
    return mapper;
  }

  /** Create Jackson object mapper with maestro client side needed features. */
  public static ObjectMapper objectMapperForClient() {
    return objectMapperIgnoringUnknown();
  }

  /** Create Jackson object mapper with FAIL_ON_UNKNOWN_PROPERTIES feature disabled. */
  public static ObjectMapper objectMapperIgnoringUnknown() {
    final ObjectMapper mapper = new ObjectMapper();
    configureMapper(mapper);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper;
  }

  /** Create Jackson object mapper for YAML with maestro needed features. */
  public static ObjectMapper objectMapperWithYaml() {
    final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    configureMapper(mapper);
    return mapper;
  }

  private static void configureMapper(ObjectMapper mapper) {
    mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_VALUES, true);
    mapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
  }

  /**
   * Static method to read a JSON to an object. It throws a non retryable error if the processing is
   * failed.
   */
  public static <T> T fromJson(ObjectMapper mapper, String json, Class<T> clazz) {
    try {
      return mapper.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      throw new MaestroInternalError(e, "Failed to parse Json due to " + e.getMessage());
    }
  }
}
