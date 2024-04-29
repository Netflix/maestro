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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

public class JsonConverterTest {
  private ObjectMapper objectMapper;
  private StringCodec stringCodec;

  @Before
  public void setup() {
    this.objectMapper = new JsonMapperProvider().get();
    this.stringCodec =
        new StringCodec(Stream.of(new GZIPCompressor()).collect(Collectors.toList()));
  }

  @Test
  public void shouldCompressTaskDataIfEnabled() throws JsonProcessingException {
    JsonConverter jsonConverter = jsonConverter(true, "gzip");
    Task task = new Task();
    task.setTaskId("testTaskId");
    task.setInputData(
        Collections.singletonMap(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    task.setOutputData(
        Collections.singletonMap(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaa"));
    WorkflowTask workflowTask = new WorkflowTask();
    workflowTask.setName("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
    task.setWorkflowTask(workflowTask);
    String json = jsonConverter.toJson(task);

    JsonNode root = objectMapper.readTree(json);
    assertEquals(
        "{\"compressed\":\"H4sIAAAAAAAAAKtWSiQWKFkRrVapFgC8tuw9WAAAAA==\",\"compressor\":\"gzip\"}",
        root.get("inputData").toString());
    assertEquals(
        "{\"compressed\":\"H4sIAAAAAAAAAKtWSsIPEuFAyYoEtbUAknwp8FkAAAA=\",\"compressor\":\"gzip\"}",
        root.get("outputData").toString());
    assertEquals(
        "{\"compressed\":\"H4sIAAAAAAAAAKVPQQ6CMBD8y545eO4VMMGgkKAn42FTF1MpbdMtREL4uyUYPuDeZnZ2ZmcG5MnI1PZOUyAQLWqmBJ4kFStrUmRiEPOyUi0OOqwMiPtjJ/KP1AOrkU5WmStyt21b67sV8QaVcUOo0WMfY/zP8h0vKrMJtLWuGslvyEQdCJB/DCRgXYgdUO+1PAU/lfalZDS/XZo6T4tjkWdRywF9yEjjBOKQQJjc+kBTnOsyh+UL0Pf/6CgBAAA=\",\"compressor\":\"gzip\"}",
        root.get("workflowTask").toString());

    Task convertedTask = jsonConverter.fromJson(json, Task.class);
    assertEquals(task, convertedTask);
  }

  @Test
  public void shouldNotCompressTaskDataIfCompressedPayloadIsLargerThanUncompressed()
      throws JsonProcessingException {
    JsonConverter jsonConverter = jsonConverter(true, "gzip");
    Task task = new Task();
    task.setTaskId("testTaskId");
    task.setInputData(Collections.singletonMap("k1", "v1"));
    task.setOutputData(Collections.singletonMap("k2", "v2"));
    String json = jsonConverter.toJson(task);

    JsonNode root = objectMapper.readTree(json);
    assertEquals("{\"k1\":\"v1\"}", root.get("inputData").toString());
    assertEquals("{\"k2\":\"v2\"}", root.get("outputData").toString());

    Task convertedTask = jsonConverter.fromJson(json, Task.class);
    assertEquals(task, convertedTask);
  }

  @Test
  public void shouldOverrideTaskCompressionAlgoIfEnabled() throws JsonProcessingException {
    JsonConverter jsonConverter = jsonConverter(true, "gzip");
    Task task = new Task();
    task.setTaskId("testTaskId");
    task.setInputData(
        Collections.singletonMap(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    task.setOutputData(
        Collections.singletonMap(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaa"));
    String json = jsonConverter.toJson(task);

    JsonNode root = objectMapper.readTree(json);
    assertEquals(
        "{\"compressed\":\"H4sIAAAAAAAAAKtWSiQeKFmRoFqpFgBUGtfvXAAAAA==\",\"compressor\":\"gzip\"}",
        root.get("inputData").toString());
    assertEquals(
        "{\"compressed\":\"H4sIAAAAAAAAAKtWSsIPEpGAkhVJqmsBFCS8Jl0AAAA=\",\"compressor\":\"gzip\"}",
        root.get("outputData").toString());

    Task convertedTask = jsonConverter.fromJson(json, Task.class);
    assertEquals(task, convertedTask);
  }

  @Test
  public void shouldNotCompressTaskDataIfCompressionDisabled() throws JsonProcessingException {
    JsonConverter jsonConverter = jsonConverter(false, "gzip");

    Task task = new Task();
    task.setTaskId("testTaskId");
    task.setInputData(
        Collections.singletonMap(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    task.setOutputData(
        Collections.singletonMap(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaa"));
    String json = jsonConverter.toJson(task);

    JsonNode root = objectMapper.readTree(json);
    assertEquals(
        "{\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}",
        root.get("inputData").toString());
    assertEquals(
        "{\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaa\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaa\"}",
        root.get("outputData").toString());

    Task convertedTask = jsonConverter.fromJson(json, Task.class);
    assertEquals(task, convertedTask);
  }

  @Test
  public void shouldSerDeTaskWhenInputAndOutputDataAreNotSetAndCompressionEnabled()
      throws JsonProcessingException {
    JsonConverter jsonConverter = jsonConverter(true, "gzip");

    Task task = new Task();
    task.setTaskId("testTaskId");

    String json = jsonConverter.toJson(task);
    Task convertedTask = jsonConverter.fromJson(json, Task.class);
    assertEquals(task, convertedTask);
  }

  @Test
  public void shouldDeserializeUnCompressedTaskDataWhenCompressionEnabled()
      throws JsonProcessingException {
    JsonConverter jsonConverter = jsonConverter(true, "gzip");

    String uncompressedTaskJson =
        "{\n"
            + "  \"inputData\": {\n"
            + "    \"k1\": \"v1\"\n"
            + "  },\n"
            + "  \"taskId\": \"testTaskId\",\n"
            + "  \"outputData\": {\n"
            + "    \"k2\": \"v2\"\n"
            + "  }\n"
            + "}";
    Task task = jsonConverter.fromJson(uncompressedTaskJson, Task.class);
    assertEquals("testTaskId", task.getTaskId());
    assertEquals(Collections.singletonMap("k1", "v1"), task.getInputData());
    assertEquals(Collections.singletonMap("k2", "v2"), task.getOutputData());
  }

  @Test
  public void shouldSerDeTaskWhenInputAndOutputDataAreEmptyAndCompressionIsEnabled()
      throws JsonProcessingException {
    JsonConverter jsonConverter = jsonConverter(true, "gzip");

    Task task = new Task();
    task.setTaskId("testTaskId");
    String json = jsonConverter.toJson(task);

    JsonNode root = objectMapper.readTree(json);
    assertEquals("{}", root.get("inputData").toString());
    assertEquals("{}", root.get("outputData").toString());

    Task convertedTask = jsonConverter.fromJson(json, Task.class);
    assertEquals(task, convertedTask);
  }

  private JsonConverter jsonConverter(boolean compressionEnabled, String compressorName) {
    return new JsonConverter(objectMapper, stringCodec, compressionEnabled, compressorName);
  }
}
