package com.netflix.maestro.signal.dao;

import static org.junit.Assert.*;

import com.netflix.maestro.signal.models.SignalInstanceRef;
import com.netflix.maestro.signal.models.SignalMatchDto;
import java.sql.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for MaestroSignalInstanceDao class.
 *
 * @author jun-he
 */
public class MaestroSignalInstanceDaoTest extends MaestroBaseSignalDaoTest {
  private MaestroSignalInstanceDao instanceDao;
  private Connection conn;

  @Before
  public void setup() throws Exception {
    instanceDao = new MaestroSignalInstanceDao(DATA_SOURCE, MAPPER, CONFIG, METRICS);
    conn = DATA_SOURCE.getConnection();
    long seqId =
        instanceDao.addSignalInstance(conn, "uuid1", "signal_a", "{\"instance_id\": \"uuid1\"}");
    conn.commit();
    assertEquals(1, seqId);
  }

  @After
  public void cleanup() throws Exception {
    conn.prepareStatement("DELETE FROM maestro_signal_instance").executeUpdate();
    conn.commit();
    conn.close();
  }

  @Test
  public void testAddSignalInstance() throws Exception {
    long seqId = instanceDao.addSignalInstance(conn, "uuid2", "signal_a", "{}");
    conn.commit();
    assertEquals(2, seqId);
  }

  @Test
  public void testAddDuplicateSignalInstance() throws Exception {
    long seqId = instanceDao.addSignalInstance(conn, "uuid1", "signal_a", "{}");
    conn.commit();
    assertEquals(-1, seqId);
  }

  @Test
  public void testGetSignalInstance() {
    assertNull(instanceDao.getSignalInstance(null));
    // get a specific signal instance
    var instance = instanceDao.getSignalInstance(new SignalInstanceRef("signal_a", 1));
    assertEquals(1, instance.getSeqId());
    assertEquals("uuid1", instance.getInstanceId());
    // get not existing signal instance
    assertNull(instanceDao.getSignalInstance(new SignalInstanceRef("signal_a", 2)));
    // get the latest signal instance
    instance = instanceDao.getSignalInstance(new SignalInstanceRef("signal_a", -1));
    assertEquals(-1, instance.getSeqId());
    assertEquals("uuid1", instance.getInstanceId());
  }

  @Test
  public void testMatchSignalDependency() {
    Long seqId = instanceDao.matchSignalDependency(new SignalMatchDto("signal_a", null));
    assertEquals(1, seqId.intValue());
    assertNull(instanceDao.matchSignalDependency(new SignalMatchDto("signal_b", null)));
  }

  @Test
  public void testMatchSignal() throws Exception {
    Long seqId = instanceDao.matchSignal(conn, new SignalMatchDto("signal_a", null), 0);
    assertEquals(1, seqId.intValue());
    assertNull(instanceDao.matchSignal(conn, new SignalMatchDto("signal_a", null), 1));
  }
}
