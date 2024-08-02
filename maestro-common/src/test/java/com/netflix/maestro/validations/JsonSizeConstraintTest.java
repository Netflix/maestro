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

import com.netflix.maestro.models.api.WorkflowCreateRequest;
import jakarta.validation.ConstraintViolation;
import java.util.Set;
import org.junit.Test;

public class JsonSizeConstraintTest extends BaseConstraintTest {
  private static class TestWorkflowCreateRequest500BytesLimit {
    @JsonSizeConstraint("500")
    WorkflowCreateRequest workflowCreateRequest;

    TestWorkflowCreateRequest500BytesLimit(WorkflowCreateRequest workflowCreateRequest) {
      this.workflowCreateRequest = workflowCreateRequest;
    }
  }

  private static class TestWorkflowCreateRequest1KBLimit {
    @JsonSizeConstraint("1KB")
    WorkflowCreateRequest workflowCreateRequest;

    TestWorkflowCreateRequest1KBLimit(WorkflowCreateRequest workflowCreateRequest) {
      this.workflowCreateRequest = workflowCreateRequest;
    }
  }

  private static class TestWorkflowCreateRequest10Point3MBLimit {
    @JsonSizeConstraint("10.3MB")
    WorkflowCreateRequest workflowCreateRequest;

    TestWorkflowCreateRequest10Point3MBLimit(WorkflowCreateRequest workflowCreateRequest) {
      this.workflowCreateRequest = workflowCreateRequest;
    }
  }

  private static class TestWorkflowCreateRequest2GBLimit {
    @JsonSizeConstraint("2GB")
    WorkflowCreateRequest workflowCreateRequest;

    TestWorkflowCreateRequest2GBLimit(WorkflowCreateRequest workflowCreateRequest) {
      this.workflowCreateRequest = workflowCreateRequest;
    }
  }

  private static class TestWorkflowCreateRequest1KBMalformed {
    @JsonSizeConstraint("1 Kilobytes")
    WorkflowCreateRequest workflowCreateRequest;

    TestWorkflowCreateRequest1KBMalformed(WorkflowCreateRequest workflowCreateRequest) {
      this.workflowCreateRequest = workflowCreateRequest;
    }
  }

  private static class TestWorkflowCreateRequest10KBMalformed {
    @JsonSizeConstraint("10K")
    WorkflowCreateRequest workflowCreateRequest;

    TestWorkflowCreateRequest10KBMalformed(WorkflowCreateRequest workflowCreateRequest) {
      this.workflowCreateRequest = workflowCreateRequest;
    }
  }

  @Test
  public void testIfSizeValidationWorks() throws Exception {
    WorkflowCreateRequest request =
        loadObject("fixtures/api/sample-workflow-create-request.json", WorkflowCreateRequest.class);
    Set<ConstraintViolation<TestWorkflowCreateRequest500BytesLimit>> violations =
        validator.validate(new TestWorkflowCreateRequest500BytesLimit(request));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflowCreateRequest500BytesLimit> violation =
        violations.iterator().next();
    assertEquals(
        "Size of class com.netflix.maestro.models.api.WorkflowCreateRequest is 639 bytes"
            + " which is larger than limit of 500 bytes",
        violation.getMessage());
  }

  @Test
  public void testSizeValidationDoesNotThrowWhenUnderSizeLimit() throws Exception {
    WorkflowCreateRequest request =
        loadObject("fixtures/api/sample-workflow-create-request.json", WorkflowCreateRequest.class);

    Set<ConstraintViolation<TestWorkflowCreateRequest1KBLimit>> violations1KB =
        validator.validate(new TestWorkflowCreateRequest1KBLimit(request));
    assertEquals(0, violations1KB.size());

    Set<ConstraintViolation<TestWorkflowCreateRequest10Point3MBLimit>> violations10Point3MB =
        validator.validate(new TestWorkflowCreateRequest10Point3MBLimit(request));
    assertEquals(0, violations10Point3MB.size());

    Set<ConstraintViolation<TestWorkflowCreateRequest2GBLimit>> violations2GB =
        validator.validate(new TestWorkflowCreateRequest2GBLimit(request));
    assertEquals(0, violations2GB.size());
  }

  @Test
  public void testSizeValidationThrowsIfValidationIsMalformed() throws Exception {
    WorkflowCreateRequest request =
        loadObject("fixtures/api/sample-workflow-create-request.json", WorkflowCreateRequest.class);

    Set<ConstraintViolation<TestWorkflowCreateRequest1KBMalformed>> violations =
        validator.validate(new TestWorkflowCreateRequest1KBMalformed(request));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflowCreateRequest1KBMalformed> violation =
        violations.iterator().next();
    assertEquals(
        "@JsonSizeConstraint(\"1 Kilobytes\") annotation is malformed. Check javadocs for JsonSizeConstraint annotation.",
        violation.getMessage());

    Set<ConstraintViolation<TestWorkflowCreateRequest10KBMalformed>> violations10KBMalformed =
        validator.validate(new TestWorkflowCreateRequest10KBMalformed(request));
    assertEquals(1, violations10KBMalformed.size());
    ConstraintViolation<TestWorkflowCreateRequest10KBMalformed> violation10KBMalformed =
        violations10KBMalformed.iterator().next();
    assertEquals(
        "@JsonSizeConstraint(\"10K\") annotation is malformed. Check javadocs for JsonSizeConstraint annotation.",
        violation10KBMalformed.getMessage());
  }
}
