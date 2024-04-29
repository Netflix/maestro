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

/** Base class for all different compression algorithm implementations like gzip/lz4. */
public interface Compressor {
  /**
   * Returns the name of Compressor implementation (eg: gzip/lz4). This string would be used by the
   * for looking up the Compressor implementation so has to be unique.
   *
   * @return compressor name
   */
  String getName();

  /**
   * Compresses the provided byte array and returns the compressed bytes.
   *
   * @param bytes byte array to compress
   * @return compressed byte array
   * @throws IOException exception while compressing the string value
   */
  byte[] compress(byte[] bytes) throws IOException;

  /**
   * Decompresses the provided bytes .
   *
   * @param bytes compressed byte array
   * @return decompressed byte array
   * @throws IOException exception while decompressing the byte array
   */
  byte[] decompress(byte[] bytes) throws IOException;
}
