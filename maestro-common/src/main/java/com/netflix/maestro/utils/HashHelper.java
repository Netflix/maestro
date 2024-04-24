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
package com.netflix.maestro.utils;

import com.netflix.maestro.exceptions.MaestroInternalError;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

/** Hash function helper class. */
public final class HashHelper {
  private static final String MD5_ALGORITHM = "MD5";
  private static final String JOIN_DELIMITER = "@";

  private static final char[] HEX_CODES = "0123456789ABCDEF".toCharArray();

  /** private constructor for utility class. */
  private HashHelper() {}

  /** Generate md5 digest for the input string joining with @. */
  public static String md5(String... inputs) {
    try {
      String input = String.join(JOIN_DELIMITER, inputs);
      MessageDigest md5 = MessageDigest.getInstance(MD5_ALGORITHM);
      byte[] bytes = md5.digest(input.getBytes(StandardCharsets.UTF_8));
      return byteToString(bytes).toLowerCase(Locale.US);
    } catch (NoSuchAlgorithmException e) {
      throw new MaestroInternalError(e, "cannot find hash algorithm: " + MD5_ALGORITHM);
    }
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private static String byteToString(byte[] bytes) {
    StringBuilder sb = new StringBuilder(2 * bytes.length);
    for (byte b : bytes) {
      sb.append(HEX_CODES[(b & 0xF0) >> 4]).append(HEX_CODES[(b & 0x0F)]);
    }
    return sb.toString();
  }
}
