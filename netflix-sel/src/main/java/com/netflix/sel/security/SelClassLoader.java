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
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;
import org.joda.time.DateTimeZone;
import org.joda.time.tz.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Customized class loader. It loads classes and resources during initialization. */
final class SelClassLoader extends ClassLoader {
  private static final Logger LOG = LoggerFactory.getLogger(SelClassLoader.class);

  static final SelClassLoader INSTANCE = new SelClassLoader();

  // hold references to avoid evicting objects during GC.
  private final List<Class<?>> preloadedClasses = new ArrayList<>();
  private final List<DateTimeZone> preloadedTimezones = new ArrayList<>();

  private SelClassLoader() {
    loadSelClasses();
  }

  private void loadSelClasses() {
    loadClassesInPackage("com.netflix.sel.ast");
    loadClassesInPackage("com.netflix.sel.type");
    loadClassesInPackage("com.netflix.sel.visitor");
    loadClassesInPackage("com.netflix.sel.ext");
    loadClassesInPackage("org.joda.time");
    ResourceBundle.getBundle("org.joda.time.format.messages");
    ResourceBundle.getBundle("sun.text.resources.FormatData");
    ResourceBundle.getBundle("sun.util.resources.CurrencyNames");

    Provider provider = DateTimeZone.getProvider(); // loads all zone info
    provider.getAvailableIDs().forEach(id -> preloadedTimezones.add(provider.getZone(id)));
  }

  private void loadClassInPackage(String path, String clazzName) {
    if (clazzName.endsWith(".class") && clazzName.startsWith(path)) {
      clazzName = clazzName.substring(0, clazzName.length() - 6).replace('/', '.');
      try {
        preloadedClasses.add(Class.forName(clazzName, true, getClass().getClassLoader()));
        LOG.info("Loaded {}.class to the class loader.", clazzName);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Invalid class: " + clazzName, e);
      }
    }
  }

  private void loadClassesInPackage(String pkgName) {
    try {
      String path = pkgName.replace('.', '/');
      Enumeration<URL> resources = getClass().getClassLoader().getResources(path);
      while (resources.hasMoreElements()) {
        URL url = resources.nextElement();
        URLConnection conn = url.openConnection();
        if (conn instanceof JarURLConnection) {
          ((JarURLConnection) conn)
              .getJarFile().stream()
                  .forEach(jarEntry -> loadClassInPackage(path, jarEntry.getName()));
        } else {
          for (String name : new File(url.toURI()).list()) {
            loadClassInPackage(path, path + "/" + name);
          }
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException("Failed to load classes in the package " + pkgName, ex);
    }
  }
}
