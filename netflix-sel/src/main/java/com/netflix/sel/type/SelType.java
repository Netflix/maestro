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

import com.netflix.sel.visitor.SelOp;

/** Interface for all supported classes */
public interface SelType {

  default Object getInternalVal() {
    throw new UnsupportedOperationException(
        getClass() + " does not support getInternalVal operation.");
  }

  default SelType assignOps(SelOp op, SelType rhs) {
    throw new UnsupportedOperationException(type() + " DO NOT support assignment operation " + op);
  }

  default SelType binaryOps(SelOp op, SelType rhs) {
    throw new UnsupportedOperationException(type() + " DO NOT support expression operation " + op);
  }

  default SelType call(String methodName, SelType[] args) {
    throw new UnsupportedOperationException(type() + " DO NOT support calling " + methodName);
  }

  default SelType field(SelString field) {
    throw new UnsupportedOperationException(type() + " DO NOT support accessing field: " + field);
  }

  default SelTypes type() {
    throw new UnsupportedOperationException(getClass() + " does not support type() operation.");
  }

  default Object unbox() {
    return getInternalVal();
  }

  // null type
  SelType NULL =
      new SelType() {
        @Override
        public SelTypes type() {
          return SelTypes.NULL;
        }

        @Override
        public Object getInternalVal() {
          return null;
        }

        @Override
        public String toString() {
          return "NULL";
        }

        @Override
        public SelType binaryOps(SelOp op, SelType rhs) {
          switch (op) {
            case EQUAL:
              return SelBoolean.of(null == rhs.getInternalVal());
            case NOT_EQUAL:
              return SelBoolean.of(null != rhs.getInternalVal());
            case LT:
            case GT:
              return SelBoolean.of(false);
            case ADD:
              return SelString.of("null" + rhs.getInternalVal());
          }
          throw new UnsupportedOperationException(
              type() + " DO NOT support expression operation " + op);
        }
      };

  // void type
  SelType VOID =
      new SelType() {
        @Override
        public SelTypes type() {
          return SelTypes.VOID;
        }

        @Override
        public String toString() {
          return "VOID";
        }

        @Override
        public Object getInternalVal() {
          return null;
        }
      };
}
