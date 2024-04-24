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
package com.netflix.maestro.validations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.utils.MapHelper;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.validation.ConstraintViolation;
import org.junit.Test;

public class RunParamsConstraintTest extends BaseConstraintTest {

  private static class TestRunParams {
    @RunParamsConstraint private final Map<String, ParamDefinition> runParams;

    TestRunParams(String name, String value) {
      this.runParams = new LinkedHashMap<>();
      runParams.put(name, ParamDefinition.buildParamDefinition(name, value));
    }

    TestRunParams(Map<String, ParamDefinition> params) {
      this.runParams = params;
    }
  }

  @Test
  public void isValidParamMap() {
    Set<ConstraintViolation<TestRunParams>> violations =
        validator.validate(new TestRunParams("foo", "bar"));
    assertEquals(0, violations.size());
    violations = validator.validate(new TestRunParams(null));
    assertEquals(0, violations.size());
    violations = validator.validate(new TestRunParams(Collections.emptyMap()));
    assertEquals(0, violations.size());
  }

  @Test
  public void isNameInvalid() {
    TestRunParams invalid = new TestRunParams("util", "bar");
    Set<ConstraintViolation<TestRunParams>> violations = validator.validate(invalid);
    assertEquals(1, violations.size());
    ConstraintViolation<TestRunParams> violation = violations.iterator().next();
    assertEquals(invalid.runParams, violation.getInvalidValue());
    assertTrue(
        violation
            .getMessage()
            .startsWith("[param definition] cannot use any of reserved param names"));
  }

  @Test
  public void isNameCaseIgnoredInChecks() {
    TestRunParams invalid = new TestRunParams("PARAMS", "bar");
    Set<ConstraintViolation<TestRunParams>> violations = validator.validate(invalid);
    assertEquals(1, violations.size());
    ConstraintViolation<TestRunParams> violation = violations.iterator().next();
    assertEquals(invalid.runParams, violation.getInvalidValue());
    assertTrue(
        violation
            .getMessage()
            .startsWith(
                "[param definition] cannot use any of reserved param names - rejected value is [PARAMS]"));

    TestRunParams valid = new TestRunParams("authorized_managers", "bar");
    assertEquals(0, validator.validate(valid).size());
  }

  @Test
  public void areThereTooManyParams() {
    TestRunParams invalid =
        new TestRunParams(
            IntStream.range(0, 200)
                .mapToObj(i -> ParamDefinition.buildParamDefinition("foo-" + i, "bar-" + i))
                .collect(MapHelper.toListMap(ParamDefinition::getName, Function.identity())));
    Set<ConstraintViolation<TestRunParams>> violations = validator.validate(invalid);
    assertEquals(1, violations.size());
    ConstraintViolation<TestRunParams> violation = violations.iterator().next();
    assertEquals(invalid.runParams, violation.getInvalidValue());
    assertEquals(
        "[param definition] contain the number of params [200] more than param size limit 128",
        violation.getMessage());

    TestRunParams valid =
        new TestRunParams(
            IntStream.range(0, Constants.PARAM_MAP_SIZE_LIMIT)
                .mapToObj(i -> ParamDefinition.buildParamDefinition("foo-" + i, "bar-" + i))
                .collect(MapHelper.toListMap(ParamDefinition::getName, Function.identity())));
    assertEquals(0, validator.validate(valid).size());
  }

  @Test
  public void isParamSourceSet() {
    TestRunParams invalid =
        new TestRunParams(
            Collections.singletonMap(
                "foo",
                StringParamDefinition.builder()
                    .name("foo")
                    .value("bar")
                    .addMetaField("source", "SYSTEM")
                    .build()));
    Set<ConstraintViolation<TestRunParams>> violations = validator.validate(invalid);
    assertEquals(1, violations.size());
    ConstraintViolation<TestRunParams> violation = violations.iterator().next();
    assertEquals(invalid.runParams, violation.getInvalidValue());
    assertEquals(
        "[param definition] users cannot set param source for a param: foo",
        violation.getMessage());
  }
}
