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
package com.netflix.sel.security;

import static org.junit.Assert.*;

import com.netflix.sel.ast.ParseException;
import com.netflix.sel.type.SelType;
import java.security.AccessControlException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelThreadTest {

  private SelThread t1, t2;
  private volatile int i = 0;
  private Throwable ex = null;

  @Before
  public void setUp() throws Exception {
    t1 =
        new SelThread(
            "test",
            new Runnable() {
              @Override
              public void run() {
                System.exit(1);
              }
            },
            10,
            10,
            10,
            16,
            100);
    t1.setUncaughtExceptionHandler(
        new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            i = -10;
            ex = e;
          }
        });
    t2 =
        new SelThread(
            "test",
            new Runnable() {
              @Override
              public void run() {
                i = 10;
              }
            },
            100,
            100,
            100,
            1000,
            100);
    t2.setUncaughtExceptionHandler(
        new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            i = -10;
            ex = e;
          }
        });
  }

  @After
  public void tearDown() throws Exception {
    System.setSecurityManager(null);
    i = 0;
    ex = null;
  }

  @Test
  public void testEvaluate() throws Exception {
    SelType res = t1.evaluate("1+1;", new HashMap<>(), null);
    assertEquals("LONG: 2", res.type() + ": " + res);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidEvaluate() throws Exception {
    t1.evaluate("Integer.valueOf(new int[1, 2]);", new HashMap<>(), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEvaluateExpressionTooLong() throws Exception {
    t1.evaluate("x.IsInvalidExpression();", new HashMap<>(), null);
  }

  @Test
  public void testValidate() throws Exception {
    Set<String> res = t1.validate("x.length;", new HashSet<>());
    assertEquals("[x]", res.toString());
  }

  @Test(expected = ParseException.class)
  public void testInvalidate() throws Exception {
    t1.validate("invalid();", new HashSet<>());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateExpressionTooLong() throws Exception {
    t1.validate("x.IsInvalidExpression();", new HashSet<>());
  }

  @Test(expected = IllegalStateException.class)
  public void testRunWithoutSecurityManager() {
    t1.run();
  }

  @Test
  public void testRunAccessDenied() throws Exception {
    System.setSecurityManager(new SelSecurityManager());
    t1.start();
    t1.join();
    assertEquals(-10, i);
    assertTrue(ex instanceof AccessControlException);
  }

  @Test
  public void testRun() throws Exception {
    assertEquals(0, i);
    System.setSecurityManager(new SelSecurityManager());
    t2.start();
    t2.join();
    assertEquals(10, i);
    assertNull(ex);
  }
}
