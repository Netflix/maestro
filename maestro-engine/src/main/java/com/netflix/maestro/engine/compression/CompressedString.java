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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.maestro.utils.Checks;
import lombok.Getter;

/**
 * POJO to represent a compressed string. During JSON serialization we retain the compressor name
 * which can then be used during deserialization to decompress.
 */
@Getter
public class CompressedString {
  private final String compressor;
  private final String compressed;

  @JsonCreator
  public CompressedString(
      @JsonProperty("compressor") String compressor,
      @JsonProperty("compressed") String compressed) {
    this.compressor = Checks.notNull(compressor, "compressor cannot be null");
    this.compressed = Checks.notNull(compressed, "compressed cannot be null");
  }
}
