/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.server.controllers;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.concurrency.TagPermitManager;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.api.TagPermitRequest;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.tagpermits.TagPermit;
import com.netflix.maestro.models.timeline.Timeline;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class TagPermitControllerTest extends MaestroBaseTest {
  @Mock private TagPermitManager tagPermitManager;
  @Mock private User.UserBuilder callerBuilder;

  private TagPermitController tagPermitController;
  private User testUser;

  @Before
  public void setUp() {
    testUser = User.builder().name("test-user").build();
    when(callerBuilder.build()).thenReturn(testUser);

    tagPermitController = new TagPermitController(tagPermitManager, callerBuilder);
  }

  @Test
  public void testUpsertTagPermit() {
    TagPermitRequest request = new TagPermitRequest();
    request.setTag("test-tag");
    request.setMaxAllowed(5);

    ResponseEntity<?> response = tagPermitController.upsertTagPermit(request);

    assertEquals(HttpStatus.OK, response.getStatusCode());

    ArgumentCaptor<String> tagCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);

    verify(tagPermitManager, times(1))
        .upsertTagPermit(tagCaptor.capture(), limitCaptor.capture(), userCaptor.capture());

    assertEquals("test-tag", tagCaptor.getValue());
    assertEquals(Integer.valueOf(5), limitCaptor.getValue());
    assertEquals(testUser, userCaptor.getValue());
  }

  @Test
  public void testGetTagPermit() {
    String tag = "existing-tag";
    TagPermit expectedTagPermit = new TagPermit(tag, 10, new Timeline(Collections.emptyList()));
    when(tagPermitManager.getTagPermit(any())).thenReturn(expectedTagPermit);

    TagPermit result = tagPermitController.getTagPermit(tag);

    assertEquals(expectedTagPermit, result);
    verify(tagPermitManager, times(1)).getTagPermit(any());
  }

  @Test
  public void testGetTagPermitNotFound() {
    when(tagPermitManager.getTagPermit(any())).thenReturn(null);
    AssertHelper.assertThrows(
        "Should not allow merging literal map param to a SEL defined param",
        MaestroNotFoundException.class,
        "No tag permit found for tag",
        () -> tagPermitController.getTagPermit("non-existent-tag"));
  }

  @Test
  public void testRemoveTagPermit() {
    ResponseEntity<?> response = tagPermitController.removeTagPermit("tag-to-remove");

    assertEquals(HttpStatus.OK, response.getStatusCode());
    verify(tagPermitManager, times(1)).removeTagPermit(any());
  }
}
