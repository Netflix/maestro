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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.validations.MaestroNameConstraint;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import lombok.Data;

/** Tag data model. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"name", "namespace", "permit", "attributes"},
    alphabetic = true)
@Data
public class Tag {
  @MaestroNameConstraint private String name;
  private Namespace namespace;

  /**
   * permit is by name for all namespaces and its value is fetched from tag permit manager at
   * runtime time.
   */
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Integer permit;

  /** Attributes associated with a tag. For example, the creator of the tag. */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Map<String, Object> attributes = new HashMap<>();

  /**
   * Static method to create a Tag object by its tag name.
   *
   * @param name tag name string
   * @return a Tag object
   */
  @JsonCreator
  public static Tag create(String name) {
    Tag tag = new Tag();
    tag.name = name;
    return tag;
  }

  /** tag name space. */
  public enum Namespace {
    /** system defined tag or parameter, e.g. RUN_TS. */
    SYSTEM,
    /** RUNTIME defined tag or parameter, e.g. workflowInstanceId. */
    RUNTIME,
    /** notebook template defined tag or parameter. */
    NOTEBOOK_TEMPLATE,
    /** USER defined tag or parameter. */
    USER_DEFINED,
    /** Platform developers defined tag. */
    PLATFORM;

    /** Static creator. */
    @JsonCreator
    public static Namespace create(String ns) {
      return Namespace.valueOf(ns.toUpperCase(Locale.US));
    }
  }

  /**
   * Get the attributes.
   *
   * @return the attributes of the tag.
   */
  @JsonAnyGetter
  public Map<String, Object> getAttributes() {
    return this.attributes;
  }

  /**
   * Add an attribute to tag attributes.
   *
   * @param key of the attributes, for example "creator".
   * @param attribute is the value of the attributes, for example, a {@link User} represent the tag
   *     creator.
   */
  @JsonAnySetter
  public void addAttribute(String key, Object attribute) {
    this.attributes.put(key, attribute);
  }
}
