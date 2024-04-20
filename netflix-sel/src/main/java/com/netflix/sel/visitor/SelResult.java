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
package com.netflix.sel.visitor;

/** Enum for return result */
public enum SelResult {
  NONE, // result for statement without data, e.g. if-else
  DATA, // result for local var, expression, etc.
  RETURN, // result for return statement
  BREAK, // result for break statement within loop
  CONTINUE, // result for continue statement within loop
  ERROR; // result for error
}
