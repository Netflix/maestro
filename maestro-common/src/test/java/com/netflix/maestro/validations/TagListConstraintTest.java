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

import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import jakarta.validation.ConstraintViolation;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class TagListConstraintTest extends BaseConstraintTest {
  private static class TestTagList {
    @TagListConstraint TagList tags;

    TestTagList(List<Tag> tags) {
      this.tags = new TagList(tags);
    }
  }

  @Test
  public void isTagListDuplicate() {
    List<Tag> tags = Arrays.asList(Tag.create("tag1"), Tag.create("tag1"));
    Set<ConstraintViolation<TestTagList>> violations = validator.validate(new TestTagList(tags));
    assertEquals(1, violations.size());
    ConstraintViolation<TestTagList> violation = violations.iterator().next();
    assertEquals("tags", violation.getPropertyPath().toString());
    assertEquals(
        "[tag list definition] cannot contain duplicate tag names", violation.getMessage());
  }

  @Test
  public void isTagListDuplicateCrossNamespaces() {
    Tag tag1 = Tag.create("tag1");
    tag1.setNamespace(Tag.Namespace.SYSTEM);
    Tag tag2 = Tag.create("tag1");
    tag2.setNamespace(Tag.Namespace.USER_DEFINED);
    Set<ConstraintViolation<TestTagList>> violations =
        validator.validate(new TestTagList(Arrays.asList(tag1, tag2)));
    assertEquals(1, violations.size());
    ConstraintViolation<TestTagList> violation = violations.iterator().next();
    assertEquals("tags", violation.getPropertyPath().toString());
    assertEquals(
        "[tag list definition] cannot contain duplicate tag names", violation.getMessage());
  }
}
