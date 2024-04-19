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
package com.netflix.conductor.cockroachdb.dao;

import static org.junit.Assert.*;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.dao.EventHandlerDAO;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class CockroachDBEventHandlerDAOTest extends CockroachDBBaseTest {

  private EventHandlerDAO dao;

  @Before
  public void setUp() {
    dao = new CockroachDBEventHandlerDAO(dataSource, objectMapper, config);
  }

  @Test
  public void testEventHandlers() {
    String event1 = "SQS::arn:account090:sqstest1";
    String event2 = "SQS::arn:account090:sqstest2";

    EventHandler eh = new EventHandler();
    eh.setName(UUID.randomUUID().toString());
    eh.setActive(false);
    EventHandler.Action action = new EventHandler.Action();
    action.setAction(EventHandler.Action.Type.start_workflow);
    action.setStart_workflow(new EventHandler.StartWorkflow());
    action.getStart_workflow().setName("workflow_x");
    eh.getActions().add(action);
    eh.setEvent(event1);

    dao.addEventHandler(eh);
    List<EventHandler> all = dao.getAllEventHandlers();
    assertNotNull(all);
    assertEquals(1, all.size());
    assertEquals(eh.getName(), all.get(0).getName());
    assertEquals(eh.getEvent(), all.get(0).getEvent());

    List<EventHandler> byEvents = dao.getEventHandlersForEvent(event1, true);
    assertNotNull(byEvents);
    assertEquals(0, byEvents.size()); // event is marked as in-active

    eh.setActive(true);
    eh.setEvent(event2);
    dao.updateEventHandler(eh);

    all = dao.getAllEventHandlers();
    assertNotNull(all);
    assertEquals(1, all.size());

    byEvents = dao.getEventHandlersForEvent(event1, true);
    assertNotNull(byEvents);
    assertEquals(0, byEvents.size());

    byEvents = dao.getEventHandlersForEvent(event2, true);
    assertNotNull(byEvents);
    assertEquals(1, byEvents.size());
  }
}
