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
package com.netflix.maestro.engine.dao;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class PropertiesUpdateTypeTest {
  @Test
  public void testUpsertTag()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    final Class<? extends PropertiesUpdateType> clazz =
        PropertiesUpdateType.ADD_WORKFLOW_TAG.getClass();
    Class<?>[] parameterTypes = {List.class, Tag.class};
    final Method upsertTagMethod = clazz.getDeclaredMethod("upsertTag", parameterTypes);
    List<Tag> tags = Collections.emptyList();

    // add two tags
    final Tag tag1 = Tag.create("dummy-tag-1");
    tag1.setNamespace(Tag.Namespace.NOTEBOOK_TEMPLATE);
    tags =
        ((TagList) upsertTagMethod.invoke(PropertiesUpdateType.ADD_WORKFLOW_TAG, tags, tag1))
            .getTags();
    assertEquals(Collections.singletonList(tag1), tags);

    final String tag2Name = "dummy-tag-2";
    final Tag tag2 = Tag.create(tag2Name);
    tag2.setNamespace(Tag.Namespace.NOTEBOOK_TEMPLATE);
    tags =
        ((TagList) upsertTagMethod.invoke(PropertiesUpdateType.ADD_WORKFLOW_TAG, tags, tag2))
            .getTags();
    assertEquals(Arrays.asList(tag1, tag2), tags);

    // update second tag
    final Tag tag2Updated = Tag.create(tag2Name);
    tag2Updated.setNamespace(Tag.Namespace.PLATFORM);
    tags =
        ((TagList) upsertTagMethod.invoke(PropertiesUpdateType.ADD_WORKFLOW_TAG, tags, tag2Updated))
            .getTags();
    assertEquals(Arrays.asList(tag1, tag2Updated), tags);
  }
}
