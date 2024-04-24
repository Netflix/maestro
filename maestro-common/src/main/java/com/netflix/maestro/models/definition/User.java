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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.utils.Checks;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Owner info, which applies to the whole workflow across all versions. */
@Builder
@Getter
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonSerialize(using = User.UserSerializer.class)
@JsonDeserialize(using = User.UserDeserializer.class)
@JsonPropertyOrder(
    value = {"name"},
    alphabetic = true)
@EqualsAndHashCode
public class User {
  private static final String NAME_KEY = "name";
  @NotNull private final String name;
  @Nullable private final Map<String, Object> extraInfo;

  /**
   * Static method to create an owner by a username string.
   *
   * @param name user name string
   * @return the owner object
   */
  public static User create(String name) {
    return User.builder().name(name).build();
  }

  /** Custom Serializer. */
  static class UserSerializer extends JsonSerializer<User> {
    @Override
    public void serialize(User value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (value.getExtraInfo() == null) {
        gen.writeString(value.name);
      } else {
        gen.writeStartObject();
        gen.writeStringField(NAME_KEY, value.name);
        for (String key : value.getExtraInfo().keySet()) {
          gen.writeObjectField(key, value.getExtraInfo().get(key));
        }
        gen.writeEndObject();
      }
    }
  }

  /** Custom Deserializer. */
  static class UserDeserializer extends JsonDeserializer<User> {
    @Override
    public User deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode value = p.getCodec().readTree(p);
      Checks.checkTrue(
          value.isTextual() || value.isObject(),
          "User value only supports String and JsonNode: " + value.getClass());
      if (value.isTextual()) {
        return User.create(value.asText());
      } else {
        UserBuilder builder = User.builder();
        Iterator<Map.Entry<String, JsonNode>> it = value.fields();
        Map<String, Object> extraInfo = new LinkedHashMap<>();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> e = it.next();
          if (e.getKey().equals(NAME_KEY)) {
            Checks.checkTrue(
                e.getValue().isTextual(),
                "User name field only supports StringNode: " + e.getValue().getClass());
            builder.name(e.getValue().asText());
          } else {
            if (!e.getValue().isNull()) {
              extraInfo.put(e.getKey(), e.getValue());
            }
          }
        }
        return builder.extraInfo(extraInfo).build();
      }
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
