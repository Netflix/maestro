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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import java.io.IOException;
import lombok.AllArgsConstructor;

/**
 * Custom JSONSerializer/JSONDeserializer to compress objects. It works by first converting the
 * provided object into a serialized JSON string, and then converts that payload into a {@link
 * CompressedString} and serialize it. This class is backwards compatible in that if during
 * deserialization it is provided with an unknown format, it returns as is.
 */
public final class CompressBeanSerDe {
  /** Context key. */
  public static final String CONTEXT = "context";

  private CompressBeanSerDe() {}

  /**
   * Custom {@link JsonSerializer} that compresses the value object if compression is enabled. It
   * does so by first serializing the provided object as a JSON string and then serializes it into a
   * JSON of type {@link CompressedString}.
   *
   * @param <T> class type to be serialized
   */
  public static class Serializer<T> extends JsonSerializer<T> {
    @Override
    public void serialize(T value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      Context context = (Context) serializers.getAttribute(CONTEXT);
      if (value == null || !context.isCompressionEnabled) {
        serializers.defaultSerializeValue(value, gen);
      } else {
        String originalJson = context.objectMapper.writeValueAsString(value);
        String compressedOriginalJson =
            context.stringCodec.compress(context.compressorName, originalJson);
        String compressedStringJson =
            context.objectMapper.writeValueAsString(
                new CompressedString(context.compressorName, compressedOriginalJson));
        // No need to write compressed value if it's larger than uncompressed.
        if (compressedStringJson.length() > originalJson.length()) {
          gen.writeRawValue(originalJson);
        } else {
          gen.writeRawValue(compressedStringJson);
        }
      }
    }
  }

  /**
   * Custom {@link JsonDeserializer} that decompresses the serialized {@link CompressedString}
   * payload and restores the original object.
   */
  @SuppressWarnings("rawtypes")
  public static class Deserializer extends JsonDeserializer implements ContextualDeserializer {
    private final JavaType javaType;

    // need it to satisfy Jackson
    public Deserializer() {
      this(null);
    }

    public Deserializer(JavaType javaType) {
      this.javaType = javaType;
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode root = p.getCodec().readTree(p);
      // reuses the parsed JSON tree from previous step
      try (JsonParser ttp = new TreeTraversingParser(root)) {
        Context context = (Context) ctxt.getAttribute(CONTEXT);
        ObjectMapper objectMapper = context.objectMapper;
        if (root.has("compressor") && root.has("compressed")) {
          CompressedString compressedString = objectMapper.readValue(ttp, CompressedString.class);
          String uncompressedString =
              context.stringCodec.decompress(
                  compressedString.getCompressor(), compressedString.getCompressed());
          return objectMapper.readValue(uncompressedString, javaType);
        }
        return objectMapper.readValue(ttp, javaType);
      }
    }

    @Override
    public JsonDeserializer<?> createContextual(
        DeserializationContext ctxt, BeanProperty property) {
      return new Deserializer(property.getType());
    }
  }

  @AllArgsConstructor
  public static class Context {
    private final ObjectMapper objectMapper;
    private final boolean isCompressionEnabled;
    private final String compressorName;
    private final StringCodec stringCodec;
  }
}
