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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
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
import lombok.experimental.SuperBuilder;

/** Abstract parameter instance. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@Getter
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
public abstract class AbstractParameter implements Parameter {
  @Setter @JsonIgnore @MaestroReferenceIdConstraint private String name;

  private final String expression;
  private final ParamValidator validator;
  @Valid @TagListConstraint private final TagList tags;
  private final ParamMode mode;

  @Setter private Long evaluatedTime;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final Map<String, Object> meta;

  protected AbstractParameter(AbstractParameterBuilder<?, ?> builder) {
    this.name = builder.name;
    this.expression = builder.expression;
    this.validator = builder.validator;
    this.tags = builder.tags;
    this.mode = builder.mode;
    this.evaluatedTime = builder.evaluatedTime;
    this.meta = builder.meta == null ? new LinkedHashMap<>() : builder.meta;
  }

  @JsonAnyGetter
  public Map<String, Object> getMeta() {
    return meta;
  }

  /** builder class with customized any setter. */
  @SuppressWarnings({"PMD.AbstractClassWithoutAbstractMethod"})
  protected abstract static class AbstractParameterBuilder<
      C extends AbstractParameter, B extends AbstractParameter.AbstractParameterBuilder<C, B>> {
    @JsonAnySetter
    public B addMetaField(String name, Object value) {
      // todo validate name and value
      if (meta == null) {
        meta = new LinkedHashMap<>();
      }
      meta.put(name, value);
      return self();
    }
  }

  /** Validation at initialization. */
  @Override
  public void validate() {
    Checks.checkTrue(getType() != null, "Parameter type must be set for parameter [%s]", name);
  }

  /** Get parameter source after merge. */
  @JsonIgnore
  @Override
  public ParamSource getSource() {
    if (meta.containsKey(Constants.METADATA_SOURCE_KEY)) {
      return ParamSource.create((String) meta.get(Constants.METADATA_SOURCE_KEY));
    } else {
      return null;
    }
  }

  @JsonIgnore
  boolean isImmutableToDefinitionWithoutValue(
      AbstractParamDefinition.AbstractParamDefinitionBuilder<?, ?> builder) {
    Checks.checkTrue(
        isEvaluated(),
        "Cannot convert an un-evaluated parameter %s to a parameter definition",
        getName());
    builder.name(getName()).validator(getValidator()).tags(getTags()).mode(getMode());
    if (!getMeta().isEmpty()) {
      builder.meta(getMeta());
    }
    return getMode() == ParamMode.IMMUTABLE;
  }
}
