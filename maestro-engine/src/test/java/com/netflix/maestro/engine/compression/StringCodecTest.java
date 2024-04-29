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

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

public class StringCodecTest {
  private StringCodec stringCodec;

  @Before
  public void setup() {
    this.stringCodec =
        new StringCodec(Stream.of(new GZIPCompressor()).collect(Collectors.toList()));
  }

  @Test
  public void testCompress() throws IOException {
    assertEquals("H4sIAAAAAAAAAEtMBAMAdCCLWwcAAAA=", stringCodec.compress("gzip", "aaaaaaa"));
    assertEquals("H4sIAAAAAAAAAEtMBAMAdCCLWwcAAAA=", stringCodec.compress(null, "aaaaaaa"));
  }

  @Test
  public void testDecompress() throws IOException {
    assertEquals("aaaaaaa", stringCodec.decompress("gzip", "H4sIAAAAAAAAAEtMBAMAdCCLWwcAAAA="));
  }

  @Test
  public void compressShouldThrowExceptionIfCompressorNotFound() {
    AssertHelper.assertThrows(
        "compress should throw exception if compressor not found",
        NullPointerException.class,
        "unknown compressorName: abcd",
        () -> stringCodec.compress("abcd", "testValue"));
  }

  @Test
  public void compressShouldThrowExceptionIfDecompressedStringIsNull() {
    AssertHelper.assertThrows(
        "compress should throw exception if decompressed string is null",
        NullPointerException.class,
        "uncompressedString cannot be null",
        () -> stringCodec.compress("abcd", null));
  }

  @Test
  public void decompressShouldThrowExceptionIfCompressorNotFound() {
    AssertHelper.assertThrows(
        "decompress should throw exception if compressor not found",
        NullPointerException.class,
        "unknown compressorName: abcd",
        () -> stringCodec.decompress("abcd", "H4sIAAAAAAAAAEtMBAMAdCCLWwcAAAA="));
  }

  @Test
  public void decompressShouldThrowExceptionIfCompressedStringIsNull() {
    AssertHelper.assertThrows(
        "decompress should throw exception if compressedString is null",
        NullPointerException.class,
        "compressedString cannot be null",
        () -> stringCodec.decompress("abcd", null));
  }
}
