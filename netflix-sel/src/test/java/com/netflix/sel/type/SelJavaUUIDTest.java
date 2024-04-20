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
package com.netflix.sel.type;

import static org.junit.Assert.*;

import org.junit.Test;

public class SelJavaUUIDTest {

  @Test
  public void testCalls() {
    SelType res = SelJavaUUID.of(null).call("randomUUID", new SelType[0]);
    assertEquals(SelTypes.UUID, res.type());

    res = res.call("toString", new SelType[0]);
    assertEquals(SelTypes.STRING, res.type());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallArg() {
    SelJavaUUID.of(null).call("randomUUID", new SelType[] {SelType.NULL});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallMethod() {
    SelJavaUUID.of(null).call("uuid", new SelType[] {});
  }
}
