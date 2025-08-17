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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.utils.Checks;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.Data;

/**
 * Workflow definition Metadata, which is not directly provided by callers but will be returned to
 * the caller.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "workflow_version_id", "create_time", "version_author", "git_info"},
    alphabetic = true)
@Data
public class Metadata {
  /** Constants for extra info key mappings. */
  private static final String SOURCE = "source";

  private static final String MANIFEST = "manifest";
  private static final String SOURCE_DEFINITION = "source_definition";
  private static final String API_VERSION = "api_version";
  private static final String DSL_CLIENT_HOSTNAME = "dsl_client_hostname";
  private static final String DSL_CLIENT_VERSION = "dsl_client_version";
  private static final String DSL_LANG = "dsl_lang";
  private static final String DSL_SOURCE = "dsl_source";
  private static final String STEP_ORDINALS = "step_ordinals";

  /** reserved fields cannot be set within extraInfo. */
  private static final Set<String> RESERVED_FIELDS =
      new HashSet<>(
          Arrays.asList(
              "workflow_id", "workflow_version_id", "git_info", "version_author", "create_time"));

  // Injected by maestro and ready only
  private String workflowId;
  private Long workflowVersionId;
  private Long createTime;

  // Set by callers, e.g. DSL client, instead of users.
  private User versionAuthor;
  private GitInfo gitInfo;
  private Map<String, Object> extraInfo = new LinkedHashMap<>();

  /** set extra info with validation. */
  public void setExtraInfo(Map<String, Object> extraInfo) {
    if (extraInfo != null) {
      Checks.checkTrue(
          RESERVED_FIELDS.stream().noneMatch(extraInfo::containsKey),
          "extra info %s cannot contain any reserved keys %s",
          extraInfo.keySet(),
          RESERVED_FIELDS);
    }
    this.extraInfo = extraInfo;
  }

  /**
   * extraInfo includes optional extra information, e.g. source_definition, api_version,
   * dsl_client_hostname, dsl_client_version, dsl_lang, dsl_source, etc.
   */
  @JsonAnyGetter
  public Map<String, Object> getExtraInfo() {
    return extraInfo;
  }

  /** Add fields to extraInfo. */
  @JsonAnySetter
  public void add(String name, Object value) {
    extraInfo.put(name, value);
  }

  /** Get source. */
  @JsonIgnore
  public String getSource() {
    return (String) extraInfo.get(SOURCE);
  }

  /** Get manifest. */
  @JsonIgnore
  public String getManifest() {
    return (String) extraInfo.get(MANIFEST);
  }

  /** Get source definition. */
  @JsonIgnore
  public String getSourceDefinition() {
    return (String) extraInfo.get(SOURCE_DEFINITION);
  }

  /** Get api version. */
  @JsonIgnore
  public String getApiVersion() {
    return (String) extraInfo.get(API_VERSION);
  }

  /** Get dsl client host name. */
  @JsonIgnore
  public String getDslClientHostName() {
    return (String) extraInfo.get(DSL_CLIENT_HOSTNAME);
  }

  /** Get dsl client version. */
  @JsonIgnore
  public String getDslClientVersion() {
    return (String) extraInfo.get(DSL_CLIENT_VERSION);
  }

  /** Get dsl lang. */
  @JsonIgnore
  public String getDslLang() {
    return (String) extraInfo.get(DSL_LANG);
  }

  /** Get dsl source. */
  @JsonIgnore
  public String getDslSource() {
    return (String) extraInfo.get(DSL_SOURCE);
  }

  /** Get step ordinal ids. */
  @JsonIgnore
  @SuppressWarnings("unchecked")
  public Map<String, Long> getStepOrdinals() {
    return (Map<String, Long>) extraInfo.get(STEP_ORDINALS);
  }
}
