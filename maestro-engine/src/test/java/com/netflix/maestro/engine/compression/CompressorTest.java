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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Test;

public class CompressorTest {

  @Test
  public void shouldCompressAndDeCompress() throws IOException {
    Compressor compressor = new GZIPCompressor();
    String compressed = "H4sIAAAAAAAA/ytJLS4JS8wpTQUA5C3qhAkAAAA=";
    String decompressed = "testValue";

    Assert.assertEquals(
        compressed,
        base64Encode(compressor.compress(decompressed.getBytes(StandardCharsets.UTF_8))));
    Assert.assertEquals(
        decompressed,
        new String(compressor.decompress(base64Decode(compressed)), StandardCharsets.UTF_8));
  }

  private static String base64Encode(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static byte[] base64Decode(String value) {
    return Base64.getDecoder().decode(value);
  }
}
