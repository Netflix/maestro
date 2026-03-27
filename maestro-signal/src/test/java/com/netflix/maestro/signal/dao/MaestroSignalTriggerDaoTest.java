package com.netflix.maestro.signal.dao;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.models.signal.SignalTriggerDto;
import com.netflix.maestro.models.trigger.SignalTrigger;
import java.sql.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for MaestroSignalTriggerDao class.
 *
 * @author jun-he
 */
public class MaestroSignalTriggerDaoTest extends MaestroBaseSignalDaoTest {
  private MaestroSignalTriggerDao triggerDao;
  private Connection conn;
  private SignalTrigger def;

  @Before
  public void setup() throws Exception {
    triggerDao = new MaestroSignalTriggerDao(DATA_SOURCE, MAPPER, CONFIG, METRICS);
    conn = DATA_SOURCE.getConnection();
    def = loadObject("fixtures/signal_triggers/signal_trigger_simple.json", SignalTrigger.class);
    assertTrue(triggerDao.addWorkflowTrigger(conn, "test-wf", "uuid1", def));
    conn.commit();
  }

  @After
  public void cleanup() throws Exception {
    triggerDao.delete("test-wf", "uuid1");
    conn.close();
  }

  @Test
  public void testAddDuplicateWorkflowTrigger() throws Exception {
    assertFalse(triggerDao.addWorkflowTrigger(conn, "test-wf", "uuid1", def));
    conn.commit();
  }

  @Test
  public void testGetSubscribedTriggers() throws Exception {
    var matches = triggerDao.getSubscribedTriggers("signal_a");
    assertEquals(1, matches.size());
    assertEquals("test-wf", matches.getFirst().getWorkflowId());
    assertEquals("uuid1", matches.getFirst().getTriggerUuid());
    assertNull(matches.getFirst().getSignalInstance());
  }

  @Test
  public void getTriggerForUpdate() throws Exception {
    SignalTriggerDto triggerDto = triggerDao.getTriggerForUpdate(conn, "test-wf", "uuid1");
    conn.commit();
    assertEquals("test-wf", triggerDto.workflowId());
    assertEquals("uuid1", triggerDto.triggerUuid());
    assertArrayEquals(new String[] {"signal_a", "signal_b"}, triggerDto.signals());
    assertArrayEquals(new Long[] {0L, 0L}, triggerDto.checkpoints());
  }

  @Test
  public void testDelete() throws Exception {
    triggerDao.delete("test-wf", "uuid1");
    assertNull(triggerDao.getTriggerForUpdate(conn, "test-wf", "uuid1"));
    conn.commit();
  }

  @Test
  public void testUpdateTriggerCheckpoints() throws Exception {
    assertTrue(triggerDao.updateTriggerCheckpoints(conn, "test-wf", "uuid1", new Long[] {1L, 2L}));
    conn.commit();
    SignalTriggerDto triggerDto = triggerDao.getTriggerForUpdate(conn, "test-wf", "uuid1");
    conn.commit();
    assertEquals("test-wf", triggerDto.workflowId());
    assertEquals("uuid1", triggerDto.triggerUuid());
    assertArrayEquals(new String[] {"signal_a", "signal_b"}, triggerDto.signals());
    assertArrayEquals(new Long[] {1L, 2L}, triggerDto.checkpoints());
  }
}
