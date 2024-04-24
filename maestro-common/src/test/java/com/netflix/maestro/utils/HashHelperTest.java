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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class HashHelperTest {

  @Test
  public void testStringMD5() {
    String digest = HashHelper.md5("hello world");
    assertEquals("5eb63bbbe01eeed093cb22bb8f5acdc3", digest);
  }

  @Test
  public void testStringsMD5() {
    String digest = HashHelper.md5("hello", "world");
    assertEquals("6c6df59fc9a6c8523031238265cba829", digest);
  }
}
