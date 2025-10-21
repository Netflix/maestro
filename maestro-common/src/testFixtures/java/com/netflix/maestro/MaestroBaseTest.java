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
package com.netflix.maestro;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.BooleanArrayParameter;
import com.netflix.maestro.models.parameter.BooleanParameter;
import com.netflix.maestro.models.parameter.DoubleArrayParameter;
import com.netflix.maestro.models.parameter.DoubleParameter;
import com.netflix.maestro.models.parameter.LongArrayParameter;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringArrayParameter;
import com.netflix.maestro.models.parameter.StringMapParameter;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.utils.JsonHelper;
import com.netflix.maestro.utils.ParamHelper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.MockitoAnnotations;

/** Maestro test base class. */
@SuppressWarnings({
  "VisibilityModifier",
  "PMD.UseVarargs",
  "PMD.TestClassWithoutTestCases",
  "PMD.LooseCoupling"
})
public class MaestroBaseTest {
  protected static final ObjectMapper MAPPER;
  protected static final ObjectMapper YAML_MAPPER;

  protected MaestroBaseTest() {}

  static {
    MAPPER = JsonHelper.objectMapper();
    YAML_MAPPER = JsonHelper.objectMapperWithYaml();
  }

  /** start up. */
  @BeforeClass
  public static void init() {}

  /** clean up. */
  @AfterClass
  public static void destroy() {}

  private AutoCloseable closeable;

  @Before
  public void openMocks() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @After
  public void releaseMocks() throws Exception {
    closeable.close();
  }

  protected <T> T loadObject(String fileName, Class<T> clazz) throws IOException {
    return MAPPER.readValue(loadJson(fileName), clazz);
  }

  protected <T> T loadObject(String fileName, TypeReference<T> ref) throws IOException {
    return MAPPER.readValue(loadJson(fileName), ref);
  }

  protected String loadJson(String fileName) throws IOException {
    try (InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)) {
      if (is == null) {
        return null;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
      }
    }
  }

  /** Convert param definition to parameter. */
  public Map<String, Parameter> toParameters(Map<String, ParamDefinition> paramDefinitions) {
    Map<String, Parameter> params = new LinkedHashMap<>();
    paramDefinitions.forEach((name, def) -> params.put(name, def.toParameter()));
    return params;
  }

  /** Build parameter for boolean array. */
  public BooleanArrayParameter buildParam(String key, boolean[] values) {
    return BooleanArrayParameter.builder()
        .name(key)
        .value(values)
        .evaluatedResult(values)
        .evaluatedTime(System.currentTimeMillis())
        .build();
  }

  /** Build parameter for boolean. */
  public BooleanParameter buildParam(String key, boolean value) {
    return BooleanParameter.builder()
        .name(key)
        .value(value)
        .evaluatedResult(value)
        .evaluatedTime(System.currentTimeMillis())
        .build();
  }

  /** Build parameter for double array. */
  public DoubleArrayParameter buildParam(String key, double[] values) {
    return DoubleArrayParameter.builder()
        .name(key)
        .value(ParamHelper.toDecimalArray(key, values))
        .evaluatedResult(values)
        .evaluatedTime(System.currentTimeMillis())
        .build();
  }

  /** Build parameter for long. */
  public LongParameter buildParam(String key, long value) {
    return LongParameter.builder()
        .name(key)
        .value(value)
        .evaluatedResult(value)
        .evaluatedTime(System.currentTimeMillis())
        .build();
  }

  /** Build parameter for double. */
  public DoubleParameter buildParam(String key, double value) {
    return DoubleParameter.builder()
        .name(key)
        .value(new BigDecimal(String.valueOf(value)))
        .evaluatedResult(value)
        .evaluatedTime(System.currentTimeMillis())
        .build();
  }

  /** Build parameter for long array. */
  public LongArrayParameter buildParam(String key, long[] values) {
    return LongArrayParameter.builder()
        .name(key)
        .value(values)
        .evaluatedResult(values)
        .evaluatedTime(System.currentTimeMillis())
        .build();
  }

  /** Build parameter for String array. */
  public StringArrayParameter buildParam(String key, String[] values) {
    return StringArrayParameter.builder()
        .name(key)
        .value(values)
        .evaluatedResult(values)
        .evaluatedTime(System.currentTimeMillis())
        .build();
  }

  /** Build parameter for StringMap. */
  public StringMapParameter buildParam(String key, Map<String, String> values) {
    return StringMapParameter.builder()
        .name(key)
        .value(values)
        .evaluatedResult(values)
        .evaluatedTime(System.currentTimeMillis())
        .build();
  }

  /** Build parameter for String. */
  public StringParameter buildParam(String key, String value) {
    return StringParameter.builder()
        .name(key)
        .value(value)
        .evaluatedResult(value)
        .evaluatedTime(System.currentTimeMillis())
        .build();
  }

  protected <T> EnumMap<StepInstance.Status, T> singletonEnumMap(
      StepInstance.Status status, T val) {
    EnumMap<StepInstance.Status, T> map = new EnumMap<>(StepInstance.Status.class);
    map.put(status, val);
    return map;
  }

  protected <T> EnumMap<WorkflowInstance.Status, T> singletonEnumMap(
      WorkflowInstance.Status status, T val) {
    EnumMap<WorkflowInstance.Status, T> map = new EnumMap<>(WorkflowInstance.Status.class);
    map.put(status, val);
    return map;
  }

  protected <T> Map<String, T> singletonMap(String key, T val) {
    Map<String, T> map = new LinkedHashMap<>();
    map.put(key, val);
    return map;
  }

  protected <T> Map<String, T> twoItemMap(String key1, T val1, String key2, T val2) {
    Map<String, T> map = new LinkedHashMap<>();
    map.put(key1, val1);
    map.put(key2, val2);
    return map;
  }

  protected <T> Map<String, T> threeItemMap(
      String key1, T val1, String key2, T val2, String key3, T val3) {
    Map<String, T> map = new LinkedHashMap<>();
    map.put(key1, val1);
    map.put(key2, val2);
    map.put(key3, val3);
    return map;
  }
}
