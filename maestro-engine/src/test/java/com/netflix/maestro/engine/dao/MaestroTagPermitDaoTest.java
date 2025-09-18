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
package com.netflix.maestro.engine.dao;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.engine.dto.StepTagPermit;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.tagpermits.TagPermit;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MaestroTagPermitDaoTest extends MaestroDaoBaseTest {
  private final String tag1 = "test-tag1";
  private final String tag2 = "test-tag2";
  private final User user = User.builder().name("tester").build();
  private final UUID uuid1 = UUID.randomUUID();
  private final UUID uuid2 = UUID.randomUUID();

  private MaestroTagPermitDao tagPermitDao;

  @Before
  public void setUp() {
    tagPermitDao = new MaestroTagPermitDao(DATA_SOURCE, MAPPER, CONFIG, metricRepo);
  }

  @After
  public void tearDown() {
    tagPermitDao.markRemoveTagPermit(tag1);
    tagPermitDao.markRemoveTagPermit(tag2);
    tagPermitDao.removeTagPermits(10);
    tagPermitDao.releaseStepTagPermit(uuid1);
    tagPermitDao.releaseStepTagPermit(uuid2);
    tagPermitDao.removeReleasedStepTagPermits(10);
  }

  private Tag createTag(String name, int permit) {
    Tag tag = Tag.create(name);
    tag.setPermit(permit);
    return tag;
  }

  @Test
  public void testUpsertTagPermit() {
    tagPermitDao.upsertTagPermit(tag1, 5, user);

    // create a new tag permit
    TagPermit result = tagPermitDao.getTagPermit(tag1);
    assertNotNull(result);
    assertEquals(tag1, result.getTag());
    assertEquals(5, result.getMaxAllowed());
    assertNotNull(result.getTimeline());
    assertEquals(1, result.getTimeline().getTimelineEvents().size());
    assertEquals(
        "User [tester] created the tag permit with limit [5]",
        result.getTimeline().getTimelineEvents().getFirst().getMessage());

    // Update with new limit
    tagPermitDao.upsertTagPermit(tag1, 10, user);
    result = tagPermitDao.getTagPermit(tag1);
    assertNotNull(result);
    assertEquals(10, result.getMaxAllowed());
    assertEquals(2, result.getTimeline().getTimelineEvents().size());
    assertEquals(
        "User [tester] updated the tag permit to the new limit [10]",
        result.getTimeline().getTimelineEvents().getLast().getMessage());
  }

  @Test
  public void testMarkRemoveTagPermit() {
    tagPermitDao.upsertTagPermit(tag1, 5, user);

    assertNotNull(tagPermitDao.getTagPermit(tag1));
    assertTrue(tagPermitDao.markRemoveTagPermit(tag1));
  }

  @Test
  public void testGetTagPermit() {
    // Tag permit should not exist
    assertNull(tagPermitDao.getTagPermit(tag1));

    tagPermitDao.upsertTagPermit(tag1, 5, user);
    assertNotNull(tagPermitDao.getTagPermit(tag1));
  }

  @Test
  public void testGetStepTagPermitStatus() {
    List<Tag> tags = Arrays.asList(createTag(tag1, 5), createTag(tag2, 3));
    var event = TimelineLogEvent.info("test event");

    int initialStatus = tagPermitDao.getStepTagPermitStatus(uuid1);
    assertEquals(-1, initialStatus);

    boolean insertResult = tagPermitDao.insertStepTagPermit(tags, uuid1, event);
    assertTrue(insertResult);

    int statusAfterInsert = tagPermitDao.getStepTagPermitStatus(uuid1);
    assertEquals(0, statusAfterInsert);

    boolean releaseResult = tagPermitDao.releaseStepTagPermit(uuid1);
    assertTrue(releaseResult);
  }

  @Test
  public void testInsertStepTagPermit() {
    List<Tag> tags = List.of(createTag(tag1, 5));
    var event = TimelineLogEvent.info("test event");

    boolean firstInsert = tagPermitDao.insertStepTagPermit(tags, uuid1, event);
    assertTrue(firstInsert);

    boolean secondInsert = tagPermitDao.insertStepTagPermit(tags, uuid1, event);
    assertFalse(secondInsert);
  }

  @Test
  public void testReleaseStepTagPermit() {
    assertFalse(tagPermitDao.releaseStepTagPermit(uuid1));
    tagPermitDao.insertStepTagPermit(List.of(), uuid1, null);
    assertTrue(tagPermitDao.releaseStepTagPermit(uuid1));
  }

  @Test
  public void testGetSyncedTagPermits() {
    tagPermitDao.upsertTagPermit(tag1, 5, user);
    tagPermitDao.markAndLoadTagPermits(1);

    // Get synced tag permits
    List<TagPermit> permits = tagPermitDao.getSyncedTagPermits("", 10);
    assertNotNull(permits);

    assertEquals(1, permits.size());
    assertEquals(tag1, permits.getFirst().getTag());
    assertEquals(5, permits.getFirst().getMaxAllowed());
  }

  @Test
  public void testRemoveTagPermits() {
    tagPermitDao.upsertTagPermit(tag1, 5, user);
    tagPermitDao.markRemoveTagPermit(tag1);

    List<String> removedTags = tagPermitDao.removeTagPermits(10);
    assertNotNull(removedTags);
    assertTrue(removedTags.contains(tag1));

    assertNull(tagPermitDao.getTagPermit(tag1));
  }

  @Test
  public void testMarkAndLoadTagPermits() {
    // Create some tag permits
    tagPermitDao.upsertTagPermit(tag1, 5, user);
    tagPermitDao.upsertTagPermit(tag2, 10, user);

    // Mark and load tag permits
    List<TagPermit> permits = tagPermitDao.markAndLoadTagPermits(10);
    assertNotNull(permits);

    assertEquals(2, permits.size());
    assertEquals(tag1, permits.getFirst().getTag());
    assertEquals(5, permits.getFirst().getMaxAllowed());
    assertEquals(tag2, permits.getLast().getTag());
    assertEquals(10, permits.getLast().getMaxAllowed());
  }

  @Test
  public void testLoadStepTagPermits() {
    List<Tag> tags = List.of(createTag(tag1, 5));

    // Insert and mark as synced
    tagPermitDao.insertStepTagPermit(tags, uuid1, null);
    tagPermitDao.markAndLoadStepTagPermits(10, 1);

    // Load step tag permits
    List<StepTagPermit> permits = tagPermitDao.loadStepTagPermits(null, 10);
    assertNotNull(permits);

    assertEquals(1, permits.size());
    assertEquals(uuid1, permits.getFirst().uuid());
    assertEquals(11, permits.getFirst().seqNum());
    assertArrayEquals(new String[] {tag1}, permits.getFirst().tags());
  }

  @Test
  public void testRemoveReleasedStepTagPermits() {
    List<Tag> tags = List.of(createTag(tag1, 5));

    // Insert step tag permit and release it
    tagPermitDao.insertStepTagPermit(tags, uuid1, null);
    tagPermitDao.releaseStepTagPermit(uuid1);

    // Remove released step tag permits
    List<UUID> removedUuids = tagPermitDao.removeReleasedStepTagPermits(10);
    assertNotNull(removedUuids);
    assertTrue(removedUuids.contains(uuid1));

    // UUID should no longer exist
    assertEquals(-1, tagPermitDao.getStepTagPermitStatus(uuid1));
  }

  @Test
  public void testMarkAndLoadStepTagPermits() {
    List<Tag> tags = List.of(createTag(tag1, 5));

    // Insert step tag permits
    tagPermitDao.insertStepTagPermit(tags, uuid1, null);
    tagPermitDao.insertStepTagPermit(tags, uuid2, null);

    // Mark and load step tag permits
    List<StepTagPermit> permits = tagPermitDao.markAndLoadStepTagPermits(0, 10);
    assertNotNull(permits);
    assertEquals(2, permits.size());
    var check1 = permits.stream().filter(p -> uuid1.equals(p.uuid())).findAny();
    assertTrue(check1.isPresent());
    assertEquals(uuid1, check1.get().uuid());
    assertEquals(1, check1.get().seqNum());
    assertArrayEquals(new String[] {tag1}, check1.get().tags());

    var check2 = permits.stream().filter(p -> uuid2.equals(p.uuid())).findAny();
    assertTrue(check2.isPresent());
    assertEquals(uuid2, check2.get().uuid());
    assertEquals(2, check2.get().seqNum());
    assertArrayEquals(new String[] {tag1}, check2.get().tags());
  }

  @Test
  public void testMarkStepTagPermitAcquired() {
    List<Tag> tags = List.of(createTag(tag1, 5));
    tagPermitDao.insertStepTagPermit(tags, uuid1, null);

    tagPermitDao.markStepTagPermitAcquired(uuid1);

    // Status should now be 7 (ACQUIRED_STATUS_CODE)
    int status = tagPermitDao.getStepTagPermitStatus(uuid1);
    assertEquals(7, status);
  }
}
