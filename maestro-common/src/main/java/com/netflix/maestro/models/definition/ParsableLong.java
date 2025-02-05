/*
 * Copyright 2025 Netflix, Inc.
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.utils.Checks;
import java.io.IOException;
import lombok.EqualsAndHashCode;

/**
 * Contains a parsable number. It contains either a positive long integer or a string that can be
 * parsed into a number. Useful for durations or other numbers that we want to represent as strings.
 */
@JsonDeserialize(using = ParsableLong.ParsableLongDeserializer.class)
@JsonSerialize(using = ParsableLong.ParsableLongSerializer.class)
@EqualsAndHashCode
public final class ParsableLong {
  @Nullable private Long longValue;
  @Nullable private String stringValue;

  /**
   * This constructor should set exactly one value between longValue and stringValue, the other
   * being null.
   */
  private ParsableLong(Long longValue, String stringValue) {
    this.longValue = longValue;
    this.stringValue = stringValue;
  }

  /** Static method to create a new ParsableLong from a long value. */
  public static ParsableLong of(long value) {
    return new ParsableLong(value, null);
  }

  /**
   * Static method to create a new ParsableLong from a String value.
   *
   * @throws NullPointerException if input value is null.
   */
  public static ParsableLong of(String value) {
    Checks.notNull(value, "ParsableLong can not be initialized with a null String value.");
    return new ParsableLong(null, value);
  }

  /** Returns true if the ParsableLong is a long. */
  public boolean isLong() {
    return longValue != null;
  }

  /** Gets the String representation of the parsable number. */
  public String asString() {
    if (stringValue == null) {
      stringValue = longValue.toString();
    }
    return stringValue;
  }

  /** Gets the int value of the ParsableLong. */
  public int asInt() {
    return (int) getLong();
  }

  /**
   * Gets the long value of the ParsableLong.
   *
   * @throws NullPointerException if the ParsableLong is not a long.
   */
  public long getLong() {
    Checks.notNull(
        longValue,
        "getLong() should not be called on ParsableLong that is not parsed yet. "
            + "Use isLong() to check if ParsableLong is a long first.");
    return longValue;
  }

  @Override
  public String toString() {
    if (isLong()) {
      return asString();
    }
    return '"' + stringValue + '"';
  }

  /** Deserializer with the validation check. */
  static class ParsableLongDeserializer extends JsonDeserializer<ParsableLong> {
    @Override
    public ParsableLong deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode value = p.getCodec().readTree(p);
      Checks.checkTrue(
          value.isNumber() || value.isTextual(),
          "ParsableLong value only supports Long or String. It does not support the type: "
              + value.getClass());
      if (value.isNumber()) {
        Checks.checkTrue(value.asLong() >= 0, "ParsableLong long value must be non-negative");
        return ParsableLong.of(value.asLong());
      }
      return ParsableLong.of(value.asText());
    }
  }

  /** Serializer for ParsableLong. */
  static class ParsableLongSerializer extends JsonSerializer<ParsableLong> {
    @Override
    public void serialize(ParsableLong value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      if (value.isLong()) {
        gen.writeNumber(value.getLong());
      } else {
        gen.writeString(value.asString());
      }
    }
  }
}
