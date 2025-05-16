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
package com.netflix.maestro.models.parameter;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.validations.MaestroReferenceIdConstraint;
import com.netflix.maestro.validations.TagListConstraint;
import jakarta.validation.Valid;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/** Abstract parameter definition. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@Getter
@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
public abstract class AbstractParamDefinition implements ParamDefinition {
  @Setter @JsonIgnore @MaestroReferenceIdConstraint private String name;

  private final String expression;
  private final ParamValidator validator;
  @Valid @TagListConstraint private final TagList tags;
  private final ParamMode mode;
  @EqualsAndHashCode.Exclude private final Map<String, Object> meta;

  @JsonAnyGetter
  public Map<String, Object> getMeta() {
    return meta;
  }

  /** Get source of parameter after merge. */
  @JsonIgnore
  @Override
  public ParamSource getSource() {
    if (meta != null && meta.containsKey(Constants.METADATA_SOURCE_KEY)) {
      return ParamSource.create((String) meta.get(Constants.METADATA_SOURCE_KEY));
    } else {
      return null;
    }
  }

  /** Get InternalParamMode. */
  @JsonIgnore
  @Override
  public InternalParamMode getInternalMode() {
    if (meta != null && meta.containsKey(Constants.METADATA_INTERNAL_PARAM_MODE)) {
      return InternalParamMode.create((String) meta.get(Constants.METADATA_INTERNAL_PARAM_MODE));
    } else {
      return null;
    }
  }

  /** Validation at initialization. */
  @Override
  public void validate() {
    Checks.checkTrue(getType() != null, "Parameter type must be set for parameter [%s]", name);
  }

  /** builder class with customized any setter. */
  @SuppressWarnings({"PMD.AbstractClassWithoutAbstractMethod"})
  protected abstract static class AbstractParamDefinitionBuilder<
      C extends AbstractParamDefinition, B extends AbstractParamDefinitionBuilder<C, B>> {

    @JsonAnySetter
    public B addMetaField(String name, Object value) {
      if (meta == null) {
        meta = new LinkedHashMap<>();
      }
      // todo validate name and value
      meta.put(name, value);
      return self();
    }
  }
}
