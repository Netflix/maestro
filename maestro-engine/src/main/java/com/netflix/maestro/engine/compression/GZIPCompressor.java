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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** GZIP implementation of {@link Compressor}. */
public class GZIPCompressor implements Compressor {
  private static final int BUFFER_SIZE = 1024;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "gzip";
  }

  /** {@inheritDoc} */
  @Override
  public byte[] compress(byte[] bytes) throws IOException {
    try (ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
        GZIPOutputStream gzipOS = new GZIPOutputStream(byteArrayOS)) {
      gzipOS.write(bytes);
      // we need to close here to flush all buffers before we can return the compressed bytes
      gzipOS.close();
      return byteArrayOS.toByteArray();
    }
  }

  /** {@inheritDoc} */
  @Override
  public byte[] decompress(byte[] bytes) throws IOException {
    try (ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
        GZIPInputStream gzipIS = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
      // copy GZIPInputStream to ByteArrayOutputStream
      byte[] buffer = new byte[BUFFER_SIZE];
      int len = gzipIS.read(buffer);
      while (len > 0) {
        byteArrayOS.write(buffer, 0, len);
        len = gzipIS.read(buffer);
      }
      return byteArrayOS.toByteArray();
    }
  }
}
