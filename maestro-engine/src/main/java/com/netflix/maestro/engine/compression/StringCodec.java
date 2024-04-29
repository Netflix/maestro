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

import com.netflix.maestro.utils.Checks;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper class which compresses/decompresses strings and make it safe to be used in JSON format as
 * the compressed bytes are base64 encoded.
 */
public class StringCodec {
  private static final String DEFAULT_COMPRESSOR_NAME = "gzip";
  private static final Charset DEFAULT_ENCODING = StandardCharsets.UTF_8;
  private final Map<String, Compressor> compressors;

  public StringCodec(Collection<Compressor> compressors) {
    Checks.checkTrue(
        compressors != null && !compressors.isEmpty(), "compressors cannot be null or empty");
    this.compressors =
        Collections.unmodifiableMap(
            compressors.stream()
                .collect(Collectors.toMap(Compressor::getName, Function.identity())));
  }

  public String compress(String compressorName, String uncompressedString) throws IOException {
    Checks.notNull(uncompressedString, "uncompressedString cannot be null");
    Compressor compressor =
        getCompressor(compressorName == null ? DEFAULT_COMPRESSOR_NAME : compressorName);
    return base64Encode(compressor.compress(uncompressedString.getBytes(DEFAULT_ENCODING)));
  }

  public String decompress(String compressorName, String compressedString) throws IOException {
    Checks.notNull(compressedString, "compressedString cannot be null");
    Compressor compressor = getCompressor(compressorName);
    return new String(compressor.decompress(base64Decode(compressedString)), DEFAULT_ENCODING);
  }

  private Compressor getCompressor(String compressorName) {
    Compressor compressor = compressors.get(compressorName);
    Checks.notNull(compressor, "unknown compressorName: %s", compressorName);
    return compressor;
  }

  private String base64Encode(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  private byte[] base64Decode(String value) {
    return Base64.getDecoder().decode(value);
  }
}
