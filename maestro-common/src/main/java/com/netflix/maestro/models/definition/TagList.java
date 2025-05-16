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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.utils.Checks;
import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;

/** wrapper class for a list of tag. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic = true)
@Data
public class TagList {
  /** singleton object for empty tag list. */
  public static final TagList EMPTY_TAG_LIST = new TagList(Collections.emptyList());

  @JsonValue @Valid private final List<Tag> tags;

  /** TagList constructor. */
  @JsonCreator
  public TagList(List<Tag> input) {
    if (input == null) {
      tags = new ArrayList<>();
    } else {
      tags = input;
    }
  }

  /** Merge tags to the tag list. */
  @JsonIgnore
  public void merge(@Nullable List<Tag> input) {
    if (input == null) {
      return;
    }
    tags.addAll(input);
    Checks.checkTrue(
        !containsDuplicate(), "Invalid tag list as there are duplicate tag names: %s", tags);
  }

  /** Check if containing duplicate tag names. */
  @JsonIgnore
  public boolean containsDuplicate() {
    Set<String> tagNames = tags.stream().map(Tag::getName).collect(Collectors.toSet());
    return tagNames.size() != tags.size();
  }
}
