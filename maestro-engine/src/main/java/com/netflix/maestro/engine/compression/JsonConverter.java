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
package com.netflix.maestro.engine.compression;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import java.util.Map;
import lombok.Getter;

/**
 * Utility class that converts Java POJO to JSON and back. If compression is enabled it compresses
 * the configured data using the provided compressor name.
 */
public class JsonConverter {
  private final ObjectReader objectReader;
  private final ObjectWriter objectWriter;
  private final ContextAttributes contextAttributes;

  public JsonConverter(
      ObjectMapper objectMapper,
      StringCodec stringCodec,
      boolean compressionEnabled,
      String compressorName) {
    ObjectMapper newObjectMapper =
        objectMapper
            .copy()
            .addMixIn(Task.class, TaskMixIn.class)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
    this.contextAttributes =
        ContextAttributes.getEmpty()
            .withSharedAttribute(
                CompressBeanSerDe.CONTEXT,
                new CompressBeanSerDe.Context(
                    newObjectMapper, compressionEnabled, compressorName, stringCodec));
    this.objectReader = newObjectMapper.reader();
    this.objectWriter = newObjectMapper.writer();
  }

  /**
   * Converts the given value object into JSON string. If compression is enabled, it compresses the
   * configured data using the provided compressor name.
   *
   * @param value a Java value object to convert to JSON
   * @return serialized JSON string
   */
  public <T> String toJson(T value) throws JsonProcessingException {
    return objectWriter.with(contextAttributes).writeValueAsString(value);
  }

  /**
   * Converts to an instance of the type clazz from the JSON string. If it encounters any JSON
   * fields that are compressed, it decompresses it and populates the objects.
   *
   * @param json serialized JSON
   * @return deserialized object of type clazz
   */
  public <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException {
    return objectReader.with(contextAttributes).forType(clazz).readValue(json);
  }

  /**
   * Converts to an instance of the type valueTypeRef from the JSON string. If it encounters any
   * JSON fields that are compressed, it decompresses it and populates the objects.
   *
   * @param json serialized JSON
   * @return deserialized object of valueTypeRef
   */
  public <T> T fromJson(String json, TypeReference<T> valueTypeRef) throws JsonProcessingException {
    return objectReader.with(contextAttributes).forType(valueTypeRef).readValue(json);
  }

  /**
   * Task mixin class to tell Jackson to use custom serializer/deserializer for input/output data &
   * workflowTask.
   */
  @Getter
  private static class TaskMixIn {
    @JsonSerialize(using = CompressBeanSerDe.Serializer.class)
    @JsonDeserialize(using = CompressBeanSerDe.Deserializer.class)
    private Map<String, Object> inputData;

    @JsonSerialize(using = CompressBeanSerDe.Serializer.class)
    @JsonDeserialize(using = CompressBeanSerDe.Deserializer.class)
    private Map<String, Object> outputData;

    @JsonSerialize(using = CompressBeanSerDe.Serializer.class)
    @JsonDeserialize(using = CompressBeanSerDe.Deserializer.class)
    private WorkflowTask workflowTask;
  }
}
