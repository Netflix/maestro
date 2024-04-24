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
package com.netflix.maestro.models.artifact;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Default artifact to store data for a workflow instance. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
@EqualsAndHashCode
@ToString
public class DefaultArtifact implements Artifact {
  private static final String VALUE_FIELD = "value";
  private final Map<String, Object> data = new LinkedHashMap<>();

  /** static creator. */
  public static DefaultArtifact create(String key, Object value) {
    DefaultArtifact artifact = new DefaultArtifact();
    artifact.add(key, value);
    return artifact;
  }

  /** get fields from data. */
  @JsonAnyGetter
  public Map<String, Object> getData() {
    return data;
  }

  /** Add fields to data. */
  @JsonAnySetter
  public void add(String name, Object value) {
    data.put(name, value);
  }

  /** get value field from data. */
  @JsonIgnore
  public Object getValue() {
    return getField(VALUE_FIELD);
  }

  /** set value field to data. */
  @JsonIgnore
  public void setValue(Object value) {
    add(VALUE_FIELD, value);
  }

  /** get field from data by the field name. */
  @JsonIgnore
  public Object getField(String name) {
    return data.get(name);
  }

  @JsonIgnore
  @Override
  public DefaultArtifact asDefault() {
    return this;
  }

  /** No need type field inside default artifact data. */
  @JsonIgnore
  @Override
  public Type getType() {
    return Type.DEFAULT;
  }
}
