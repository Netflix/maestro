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

import java.io.File;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessControlException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelSecurityManagerTest {

  private Thread t = new Thread(() -> System.setSecurityManager(null));

  @Before
  public void setUp() throws Exception {
    SelSecurityManager ssm = new SelSecurityManager();
    ssm.setAccessControl(SelAccessController.INSTANCE.accessControlContext());
    Thread.currentThread().setContextClassLoader(SelClassLoader.INSTANCE);
    System.setSecurityManager(ssm);
  }

  @After
  public void tearDown() throws Exception {
    t.start();
    t.join();
  }

  @Test(expected = AccessControlException.class)
  public void testNoAccessSetSecurityManager() {
    System.setSecurityManager(null);
  }

  @Test(expected = AccessControlException.class)
  public void testNoAccessToCreateThread() {
    new Thread();
  }

  @Test(expected = AccessControlException.class)
  public void testNoAccessToSetClassLoader() {
    Thread.currentThread().setContextClassLoader(null);
  }

  @Test(expected = NoClassDefFoundError.class)
  public void testNoAccessToCreateClassLoader() {
    new ClassLoader() {};
  }

  @Test
  public void testHasAccessGetSecurityManager() {
    System.getSecurityManager();
  }

  @Test(expected = AccessControlException.class)
  public void testNoAccessDisk() {
    new File("/tmp/").list();
  }

  @Test(expected = MalformedURLException.class)
  public void testNoAccessURL() throws Exception {
    new URL("http://foo.bar");
  }

  @Test(expected = AccessControlException.class)
  public void testNoAccessNetwork() throws Exception {
    InetAddress.getByName("www.netflix.com");
  }

  @Test(expected = AccessControlException.class)
  public void testNoAccessProcess() throws Exception {
    Runtime.getRuntime().exec("ping www.netflix.com");
  }

  @Test(expected = AccessControlException.class)
  public void testRemoveAccessControl() throws Exception {
    Field f = SelSecurityManager.class.getDeclaredField("accs");
    f.setAccessible(true);
    f.set(System.getSecurityManager(), new ThreadLocal<AccessControlContext>());
    System.setSecurityManager(null);
  }
}
