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

import java.io.FileDescriptor;
import java.net.InetAddress;
import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.Permission;

/** Security manager to check permissions only for threads enabled access control. */
public final class SelSecurityManager extends SecurityManager {
  private final ThreadLocal<AccessControlContext> accs =
      new ThreadLocal<AccessControlContext>() {
        @Override
        public void set(AccessControlContext value) {
          if (get() != null) {
            throw new AccessControlException("Once set, cannot change thread's access control");
          }
          super.set(value);
        }
      };

  public SelSecurityManager() {}

  void setAccessControl(AccessControlContext acc) {
    accs.set(acc);
  }

  @Override
  public void checkPermission(Permission perm) {
    AccessControlContext acc = accs.get();
    if (acc != null) {
      acc.checkPermission(perm);
    }
  }

  // special handling for creating/modifying threads
  @Override
  public void checkAccess(Thread t) {
    if (accs.get() != null) {
      checkPermission(new RuntimePermission("modifyThread"));
    }
  }

  // special handling for creating/modifying thread group
  @Override
  public void checkAccess(ThreadGroup g) {
    if (accs.get() != null) {
      checkPermission(new RuntimePermission("modifyThreadGroup"));
    }
  }

  @Override
  public void checkPermission(Permission perm, Object context) {
    if (accs.get() != null) {
      super.checkPermission(perm, context);
    }
  }

  @Override
  public void checkCreateClassLoader() {
    if (accs.get() != null) {
      super.checkCreateClassLoader();
    }
  }

  @Override
  public void checkExit(int status) {
    if (accs.get() != null) {
      super.checkExit(status);
    }
  }

  @Override
  public void checkExec(String cmd) {
    if (accs.get() != null) {
      super.checkExec(cmd);
    }
  }

  @Override
  public void checkLink(String lib) {
    if (accs.get() != null) {
      super.checkLink(lib);
    }
  }

  @Override
  public void checkRead(FileDescriptor fd) {
    if (accs.get() != null) {
      super.checkRead(fd);
    }
  }

  @Override
  public void checkRead(String file) {
    if (accs.get() != null) {
      super.checkRead(file);
    }
  }

  @Override
  public void checkRead(String file, Object context) {
    if (accs.get() != null) {
      super.checkRead(file, context);
    }
  }

  @Override
  public void checkWrite(FileDescriptor fd) {
    if (accs.get() != null) {
      super.checkWrite(fd);
    }
  }

  @Override
  public void checkWrite(String file) {
    super.checkWrite(file);
  }

  @Override
  public void checkDelete(String file) {
    if (accs.get() != null) {
      super.checkDelete(file);
    }
  }

  @Override
  public void checkConnect(String host, int port) {
    if (accs.get() != null) {
      super.checkConnect(host, port);
    }
  }

  @Override
  public void checkConnect(String host, int port, Object context) {
    if (accs.get() != null) {
      super.checkConnect(host, port, context);
    }
  }

  @Override
  public void checkListen(int port) {
    if (accs.get() != null) {
      super.checkListen(port);
    }
  }

  @Override
  public void checkAccept(String host, int port) {
    if (accs.get() != null) {
      super.checkAccept(host, port);
    }
  }

  @Override
  public void checkMulticast(InetAddress maddr) {
    if (accs.get() != null) {
      super.checkMulticast(maddr);
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public void checkMulticast(InetAddress maddr, byte ttl) {
    if (accs.get() != null) {
      super.checkMulticast(maddr, ttl);
    }
  }

  @Override
  public void checkPropertiesAccess() {
    if (accs.get() != null) {
      super.checkPropertiesAccess();
    }
  }

  @Override
  public void checkPropertyAccess(String key) {
    if (accs.get() != null) {
      super.checkPropertyAccess(key);
    }
  }

  @Override
  public void checkPrintJobAccess() {
    if (accs.get() != null) {
      super.checkPrintJobAccess();
    }
  }

  @Override
  public void checkPackageAccess(String pkg) {
    if (accs.get() != null) {
      super.checkPackageAccess(pkg);
    }
  }

  @Override
  public void checkPackageDefinition(String pkg) {
    if (accs.get() != null) {
      super.checkPackageDefinition(pkg);
    }
  }

  @Override
  public void checkSetFactory() {
    if (accs.get() != null) {
      super.checkSetFactory();
    }
  }

  @Override
  public void checkSecurityAccess(String target) {
    if (accs.get() != null) {
      super.checkSecurityAccess(target);
    }
  }
}
