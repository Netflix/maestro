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

import com.netflix.sel.type.SelType;
import com.netflix.sel.type.SelTypeUtil;
import com.netflix.sel.type.SelTypes;
import java.util.Arrays;

/**
 * Abstract Param extension class. It assumes input arguments (if present) are always string type.
 */
public abstract class AbstractParamExtension implements Extension {

  public SelType call(String methodName, SelType[] args) {
    if (allStringArgs(args)) {
      if (args.length == 0) {
        return SelTypeUtil.box(callWithoutArg(methodName));
      } else if (args.length == 1) {
        return SelTypeUtil.box(callWithOneArg(methodName, args[0].toString()));
      } else if (args.length == 2) {
        return SelTypeUtil.box(callWithTwoArgs(methodName, args[0].toString(), args[1].toString()));
      } else if (args.length == 3) {
        return SelTypeUtil.box(
            callWithThreeArgs(
                methodName, args[0].toString(), args[1].toString(), args[2].toString()));
      }
    }
    throw new UnsupportedOperationException(
        "ParamExtension DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }

  /**
   * Abstract method without any argument to override for external system
   *
   * @param methodName method name to call
   * @return the result value
   */
  protected abstract Object callWithoutArg(String methodName);

  /**
   * Abstract method with one argument to override for external system
   *
   * @param methodName method name to call
   * @param arg1 input argument
   * @return the result value
   */
  protected abstract Object callWithOneArg(String methodName, String arg1);

  /**
   * Abstract method with one argument to override for external system
   *
   * @param methodName method name to call
   * @param arg1 input argument one
   * @param arg2 input argument two
   * @return the result value
   */
  protected abstract Object callWithTwoArgs(String methodName, String arg1, String arg2);

  /**
   * Abstract method accepting one argument to override for external system
   *
   * @param methodName method name to call
   * @param arg1 input argument one
   * @param arg2 input argument two
   * @param arg3 input argument three
   * @return the result value
   */
  protected abstract Object callWithThreeArgs(
      String methodName, String arg1, String arg2, String arg3);

  private boolean allStringArgs(SelType[] args) {
    if (args == null) {
      return false;
    }
    for (SelType arg : args) {
      if (arg.type() != SelTypes.STRING) {
        return false;
      }
    }
    return true;
  }
}
