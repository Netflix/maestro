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
package com.netflix.maestro.engine.concurrency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroTagPermitDao;
import com.netflix.maestro.engine.tasks.MaestroTagPermitTask;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.tagpermits.TagPermit;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.models.MessageDto;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroTagPermitManagerTest extends MaestroEngineBaseTest {
  @Mock private MaestroTagPermitDao tagPermitDao;
  @Mock private MaestroQueueSystem queueSystem;

  private MaestroTagPermitManager tagPermitManager;

  private final UUID uuid = UUID.randomUUID();
  private final String stepUuid = uuid.toString();
  private final String tag = "test-tag";

  @Before
  public void setUp() {
    tagPermitManager = new MaestroTagPermitManager(tagPermitDao, queueSystem);
  }

  private Tag createTag(String name, int permit) {
    Tag tag = Tag.create(name);
    tag.setPermit(permit);
    return tag;
  }

  @Test
  public void testAcquireFirstTimeInsertSuccess() {
    List<Tag> tags = List.of(createTag("tag1", 5), createTag("tag2", 3));
    var event = TimelineLogEvent.info("test event");

    when(tagPermitDao.getStepTagPermitStatus(uuid)).thenReturn(-1); // NOT_FOUND
    when(tagPermitDao.insertStepTagPermit(tags, uuid, event)).thenReturn(true);

    TagPermitManager.Status status = tagPermitManager.acquire(tags, stepUuid, event);

    assertFalse(status.success());
    assertEquals("The step is still waiting for tag permit", status.message());

    verify(tagPermitDao, times(1)).getStepTagPermitStatus(uuid);
    verify(tagPermitDao, times(1)).insertStepTagPermit(tags, uuid, event);
    verify(queueSystem, times(1)).notify(any(MessageDto.class));
  }

  @Test
  public void testAcquireAlreadyAcquired() {
    List<Tag> tags = List.of(createTag("tag1", 5));
    var event = TimelineLogEvent.info("test event");

    when(tagPermitDao.getStepTagPermitStatus(uuid))
        .thenReturn(MaestroTagPermitTask.ACQUIRED_STATUS_CODE);

    TagPermitManager.Status status = tagPermitManager.acquire(tags, stepUuid, event);

    assertTrue(status.success());
    assertEquals("Tag permit acquired", status.message());

    verify(tagPermitDao, times(1)).getStepTagPermitStatus(uuid);
    verifyNoInteractions(queueSystem);
  }

  @Test
  public void testAcquireWithEmptyTagsList() {
    TagPermitManager.Status status = tagPermitManager.acquire(null, "test-uuid", null);

    assertTrue(status.success());
    assertEquals(
        "No tags associated with the step, skip the tag permit acquisition", status.message());

    status = tagPermitManager.acquire(Collections.emptyList(), "test-uuid", null);

    assertTrue(status.success());
    assertEquals(
        "No tags associated with the step, skip the tag permit acquisition", status.message());

    verifyNoInteractions(tagPermitDao, queueSystem);
  }

  @Test
  public void testReleaseTagPermitsSuccess() {
    when(tagPermitDao.releaseStepTagPermit(uuid)).thenReturn(true);

    tagPermitManager.releaseTagPermits(stepUuid);

    verify(tagPermitDao, times(1)).releaseStepTagPermit(uuid);
    verify(queueSystem, times(1)).notify(any(MessageDto.class));
  }

  @Test
  public void testReleaseTagPermitsFailure() {
    when(tagPermitDao.releaseStepTagPermit(uuid)).thenReturn(false);

    tagPermitManager.releaseTagPermits(stepUuid);

    verify(tagPermitDao, times(1)).releaseStepTagPermit(uuid);
    verifyNoInteractions(queueSystem);
  }

  @Test
  public void testUpsertTagPermit() {
    int limit = 10;
    User user = User.builder().name("test-user").build();

    tagPermitManager.upsertTagPermit(tag, limit, user);

    verify(tagPermitDao, times(1)).upsertTagPermit(tag, limit, user);
    verify(queueSystem, times(1)).notify(any(MessageDto.class));
  }

  @Test
  public void testRemoveTagPermitSuccess() {
    when(tagPermitDao.markRemoveTagPermit(tag)).thenReturn(true);

    tagPermitManager.removeTagPermit(tag);

    verify(tagPermitDao, times(1)).markRemoveTagPermit(tag);
    verify(queueSystem, times(1)).notify(any(MessageDto.class));
  }

  @Test
  public void testGetTagPermit() {
    TagPermit expectedPermit = new TagPermit(tag, 5, new Timeline(Collections.emptyList()));
    when(tagPermitDao.getTagPermit(tag)).thenReturn(expectedPermit);

    TagPermit result = tagPermitManager.getTagPermit(tag);

    assertEquals(expectedPermit, result);
    verify(tagPermitDao, times(1)).getTagPermit(tag);
  }
}
