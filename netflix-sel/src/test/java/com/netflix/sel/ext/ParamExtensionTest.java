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
package com.netflix.sel.ext;

import static org.junit.Assert.*;

import com.netflix.sel.type.SelLong;
import com.netflix.sel.type.SelString;
import com.netflix.sel.type.SelType;
import org.junit.Before;
import org.junit.Test;

public class ParamExtensionTest {

  static class TestParamExtension extends AbstractParamExtension {
    protected Object getFromForeach(String foreachStepId, String stepId, String paramName) {
      return new Long[] {123L};
    }

    @Override
    protected Object callWithThreeArgs(String methodName, String arg0, String arg1, String arg2) {
      if ("getFromForeach".equals(methodName)) {
        Object res = getFromForeach(arg0, arg1, arg2);
        if (res.getClass().isArray()) {
          return res;
        } else {
          throw new IllegalStateException(
              methodName + " must return an array instead of " + res.getClass());
        }
      }
      throw new UnsupportedOperationException(
          "ParamExtension DO NOT support calling method: " + methodName + " with args");
    }

    @Override
    protected String callWithTwoArgs(String methodName, String id, String paramName) {
      if ("getFromStep".equals(methodName)) {
        return "hello";
      } else if ("getFromSignal".equals(methodName)) {
        return "world";
      }
      throw new UnsupportedOperationException("don't support method: " + methodName);
    }

    @Override
    protected Long callWithoutArg(String methodName) {
      if ("nextUniqueId".equals(methodName)) {
        return 12345L;
      }
      throw new UnsupportedOperationException("don't support method: " + methodName);
    }

    @Override
    protected Object callWithOneArg(String methodName, String fieldName) {
      if ("getFromInstance".equals(methodName)) {
        return "foo";
      }
      throw new UnsupportedOperationException("don't support method: " + methodName);
    }
  }

  static class BadParamExtension extends TestParamExtension {
    @Override
    protected Long getFromForeach(String foreachStepId, String stepId, String paramName) {
      return 123L;
    }
  }

  static class UncastParamExtension extends TestParamExtension {
    @Override
    protected Object getFromForeach(String foreachStepId, String stepId, String paramName) {
      return new char[] {'a', 'b'};
    }
  }

  private AbstractParamExtension extension;

  @Before
  public void setUp() throws Exception {
    extension = new TestParamExtension();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCallNonexistingMethod() {
    extension.call("foo", new SelType[] {SelString.of("bar"), SelString.of("bar")});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCallUnsupportedArgumentType() {
    extension.call("getFromStep", new SelType[] {SelLong.of("1"), SelLong.of("123")});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCallUnsupportedArguments() {
    extension.call("getFromStep", new SelType[] {SelString.of("foo")});
  }

  @Test(expected = IllegalStateException.class)
  public void testCallBadForeachReturn() {
    new BadParamExtension()
        .call(
            "getFromForeach",
            new SelType[] {SelString.of("foo"), SelString.of("bar"), SelString.of("bat")});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCallUncastForeachReturn() {
    new UncastParamExtension()
        .call(
            "getFromForeach",
            new SelType[] {SelString.of("foo"), SelString.of("bar"), SelString.of("bat")});
  }

  @Test
  public void testCallGetFromStep() {
    assertEquals(
        SelString.of("hello"),
        extension.call("getFromStep", new SelType[] {SelString.of("foo"), SelString.of("bar")}));
  }

  @Test
  public void testCallGetFromSignal() {
    assertEquals(
        SelString.of("world"),
        extension.call("getFromSignal", new SelType[] {SelString.of("foo"), SelString.of("bar")}));
  }

  @Test
  public void testCallGetFromForeach() {
    assertArrayEquals(
        new long[] {123},
        (long[])
            extension
                .call(
                    "getFromForeach",
                    new SelType[] {SelString.of("foo"), SelString.of("bar"), SelString.of("bat")})
                .unbox());
  }

  @Test
  public void testCallNextUniqueId() {
    assertEquals("12345", extension.call("nextUniqueId", new SelType[0]).toString());
  }

  @Test
  public void testCallGetFromInstance() {
    assertEquals(
        "foo", extension.call("getFromInstance", new SelType[] {SelString.of("bar")}).toString());
  }
}
