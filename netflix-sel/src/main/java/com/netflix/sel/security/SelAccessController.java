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

import java.security.AccessControlContext;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.SecurityPermission;
import java.util.PropertyPermission;

/** Singleton class to define the permissions allowed for SEL */
public enum SelAccessController {
  INSTANCE;

  AccessControlContext accessControlContext() {
    return this.accessControlContext;
  }

  private final AccessControlContext accessControlContext;

  SelAccessController() {
    Permissions perms = new Permissions();

    // add necessary permission here
    perms.add(
        new SecurityPermission("getProperty.package.access")); // load classes during execution
    perms.add(new PropertyPermission("line.separator", "read"));

    this.accessControlContext =
        new AccessControlContext(new ProtectionDomain[] {new ProtectionDomain(null, perms)});
  }
}
