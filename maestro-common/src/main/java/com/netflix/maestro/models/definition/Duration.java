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

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.utils.Checks;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import lombok.Data;

/** Duration data model for workflow and step timeout configurations. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonDeserialize(using = Duration.DurationDeserializer.class)
@Data
public class Duration {
  /** It is either a string or a number in the time unit of seconds. */
  @JsonValue @NotNull private final JsonNode value;

  /** Deserializer with the validation check. */
  static class DurationDeserializer extends JsonDeserializer<Duration> {
    @Override
    public Duration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode value = p.getCodec().readTree(p);
      Checks.checkTrue(
          value.isNumber() || value.isTextual(),
          "Duration value only supports Long or String. It does not support the type: "
              + value.getClass());
      if (value.isNumber()) {
        Checks.checkTrue(value.asLong() >= 0, "Duration as long must be non-negative");
      }
      return new Duration(value);
    }
  }
}
