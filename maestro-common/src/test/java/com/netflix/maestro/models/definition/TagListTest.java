/*
 * Copyright 2025 Netflix, Inc.
 * Copyright 2026 Anthropic, PBC
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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.netflix.maestro.AssertHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class TagListTest {

  @Test
  public void testConstructorCopiesInputList() {
    List<Tag> input = new ArrayList<>(List.of(Tag.create("foo")));
    TagList tagList = new TagList(input);
    tagList.merge(List.of(Tag.create("bar")));
    // the merge updates the tag list but never the caller's input list
    assertEquals(2, tagList.getTags().size());
    assertEquals(1, input.size());
    input.add(Tag.create("baz"));
    assertEquals(2, tagList.getTags().size());
  }

  @Test
  public void testMergeKeepsExistingTagOnDuplicateNames() {
    Tag existingFoo = Tag.create("foo");
    existingFoo.addAttribute("creator", "tester");
    TagList tagList = new TagList(List.of(existingFoo, Tag.create("bar")));
    Tag inputFoo = Tag.create("foo");
    inputFoo.addAttribute("creator", "runtime");
    List<Tag> input = List.of(inputFoo, Tag.create("baz"));
    tagList.merge(input);
    assertEquals(
        List.of("foo", "bar", "baz"),
        tagList.getTags().stream().map(Tag::getName).collect(Collectors.toList()));
    // the existing tag and its attributes win over the same-named input tag
    assertSame(existingFoo, tagList.getTags().get(0));
    assertEquals("tester", tagList.getTags().get(0).getAttributes().get("creator"));
    // the input list is not modified
    assertEquals(2, input.size());
  }

  @Test
  public void testMergeStillRejectsDuplicatesWithinInput() {
    TagList tagList = new TagList(null);
    AssertHelper.assertThrows(
        "duplicate tag names within the input are still rejected",
        IllegalArgumentException.class,
        "Invalid tag list as there are duplicate tag names",
        () -> tagList.merge(List.of(Tag.create("foo"), Tag.create("foo"))));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testEmptyTagListSingletonIsImmutable() {
    TagList.EMPTY_TAG_LIST.merge(List.of(Tag.create("foo")));
  }

  @Test
  public void testEmptyTagListSingletonAllowsNoOpMerges() {
    // merging nothing into the singleton is a no-op and does not throw, same as upstream
    TagList.EMPTY_TAG_LIST.merge(null);
    TagList.EMPTY_TAG_LIST.merge(List.of());
    assertEquals(0, TagList.EMPTY_TAG_LIST.getTags().size());
    // using the singleton as a merge source or copy source is always safe
    TagList target = new TagList(List.of(Tag.create("foo")));
    target.merge(TagList.EMPTY_TAG_LIST.getTags());
    assertEquals(1, target.getTags().size());
    TagList copied = new TagList(TagList.EMPTY_TAG_LIST.getTags());
    copied.merge(List.of(Tag.create("bar")));
    assertEquals(1, copied.getTags().size());
    assertEquals(0, TagList.EMPTY_TAG_LIST.getTags().size());
  }
}
