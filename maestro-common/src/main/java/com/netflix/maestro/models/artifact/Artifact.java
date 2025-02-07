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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.Constants;

/** Artifact interface. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    defaultImpl = DefaultArtifact.class)
@JsonSubTypes({
  @JsonSubTypes.Type(name = "DEFAULT", value = DefaultArtifact.class),
  @JsonSubTypes.Type(name = "SUBWORKFLOW", value = SubworkflowArtifact.class),
  @JsonSubTypes.Type(name = "FOREACH", value = ForeachArtifact.class),
  @JsonSubTypes.Type(name = "TITUS", value = TitusArtifact.class),
  @JsonSubTypes.Type(name = "NOTEBOOK", value = NotebookArtifact.class),
  @JsonSubTypes.Type(name = "DYNAMIC_OUTPUT", value = DynamicOutputArtifact.class)
})
public interface Artifact {
  /** Get artifact type info. */
  Type getType();

  /** supported artifact types. */
  enum Type {
    /** default artifact holding a (key, value) map. */
    DEFAULT(null),
    /** subworkflow artifact. */
    SUBWORKFLOW(Constants.MAESTRO_PREFIX + "subworkflow"),
    /** foreach artifact. */
    FOREACH(Constants.MAESTRO_PREFIX + "foreach"),
    /** titus artifact. */
    TITUS(Constants.MAESTRO_PREFIX + "titus"),
    /** notebook artifact. */
    NOTEBOOK(Constants.MAESTRO_PREFIX + "notebook"),
    /** dynamic output artifact. */
    DYNAMIC_OUTPUT(Constants.MAESTRO_PREFIX + "dynamic_output");

    private final String key;

    Type(String key) {
      this.key = key;
    }

    public String key() {
      return key;
    }
  }

  /**
   * get DEFAULT type artifact.
   *
   * @return concrete artifact object.
   */
  default DefaultArtifact asDefault() {
    throw new MaestroInternalError("Artifact type [%s] cannot be used as DEFAULT", getType());
  }

  /**
   * get SubworkflowArtifact type artifact.
   *
   * @return concrete artifact object.
   */
  default SubworkflowArtifact asSubworkflow() {
    throw new MaestroInternalError("Artifact type [%s] cannot be used as SUBWORKFLOW", getType());
  }

  /**
   * get ForeachArtifact type artifact.
   *
   * @return concrete artifact object.
   */
  default ForeachArtifact asForeach() {
    throw new MaestroInternalError("Artifact type [%s] cannot be used as FOREACH", getType());
  }

  /**
   * get Titus type artifact.
   *
   * @return concrete artifact object.
   */
  default TitusArtifact asTitus() {
    throw new MaestroInternalError("Artifact type [%s] cannot be used as TITUS", getType());
  }

  /**
   * get Notebook type artifact.
   *
   * @return concrete artifact object.
   */
  default NotebookArtifact asNotebook() {
    throw new MaestroInternalError("Artifact type [%s] cannot be used as NOTEBOOK", getType());
  }

  /**
   * get DynamicOutput type artifact.
   *
   * @return concrete artifact object.
   */
  default DynamicOutputArtifact asDynamicOutput() {
    throw new MaestroInternalError(
        "Artifact type [%s] cannot be used as DYNAMIC_OUTPUT", getType());
  }
}
