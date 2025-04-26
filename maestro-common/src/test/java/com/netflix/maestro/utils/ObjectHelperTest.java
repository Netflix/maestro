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
package com.netflix.maestro.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.junit.Assert;
import org.junit.Test;

public class ObjectHelperTest {

  @Test
  public void testValueOrDefault() {
    String defaultValue = "default";
    Assert.assertEquals(defaultValue, ObjectHelper.valueOrDefault(null, defaultValue));
    Assert.assertEquals("value", ObjectHelper.valueOrDefault("value", defaultValue));
  }

  @Test
  public void testCollectionsNullOrEmpty() {
    Assert.assertTrue(ObjectHelper.isCollectionEmptyOrNull(null));
    Assert.assertTrue(ObjectHelper.isCollectionEmptyOrNull(Collections.emptyList()));
    Assert.assertFalse(ObjectHelper.isCollectionEmptyOrNull(Collections.singletonList("abc")));
  }

  @Test
  public void testToNumeric() {
    Assert.assertEquals(OptionalLong.empty(), ObjectHelper.toNumeric(null));
    Assert.assertEquals(OptionalLong.empty(), ObjectHelper.toNumeric("not a number"));
    Assert.assertEquals(OptionalLong.of(123), ObjectHelper.toNumeric("123"));
  }

  @Test
  public void testToDouble() {
    Assert.assertEquals(OptionalDouble.empty(), ObjectHelper.toDouble(null));
    Assert.assertEquals(OptionalDouble.empty(), ObjectHelper.toDouble("not a number"));
    Assert.assertEquals(OptionalDouble.of(123.45), ObjectHelper.toDouble("123.45"));
  }

  @Test
  public void testIsNullOrEmpty() {
    Assert.assertTrue(ObjectHelper.isNullOrEmpty(null));
    Assert.assertTrue(ObjectHelper.isNullOrEmpty(""));
    Assert.assertFalse(ObjectHelper.isNullOrEmpty("foo"));
  }

  @Test
  public void testPartitionList() {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      list.add("i" + i);
    }
    Assert.assertEquals(2, ObjectHelper.partitionList(list, 3).size());
    Assert.assertEquals(List.of("i0", "i1", "i2"), ObjectHelper.partitionList(list, 3).get(0));
    Assert.assertEquals(List.of("i3", "i4"), ObjectHelper.partitionList(list, 3).get(1));
    Assert.assertTrue(ObjectHelper.partitionList(null, 5).isEmpty());
    Assert.assertEquals(List.of(list), ObjectHelper.partitionList(list, 5));
    Assert.assertEquals(5, ObjectHelper.partitionList(list, 1).size());
  }

  @Test
  public void testPartitionEmptyList() {
    Assert.assertTrue(ObjectHelper.partitionList(new ArrayList<>(), 3).isEmpty());
  }
}
