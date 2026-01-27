package com.netflix.maestro.signal.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.models.signal.SignalMatchDto;
import com.netflix.maestro.models.signal.SignalOperator;
import com.netflix.maestro.models.signal.SignalParamDto;
import com.netflix.maestro.models.signal.SignalParamValue;
import java.sql.Connection;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for MaestroSignalParamDao class.
 *
 * @author jun-he
 */
public class MaestroSignalParamDaoTest extends MaestroBaseSignalDaoTest {
  private MaestroSignalParamDao paramDao;
  private Connection conn;

  @Before
  public void setup() throws Exception {
    paramDao = new MaestroSignalParamDao(DATA_SOURCE, MAPPER, CONFIG, METRICS);
    conn = DATA_SOURCE.getConnection();
    var added =
        paramDao.addSignalParams(
            conn,
            List.of(
                new SignalParamDto("signal_a", "foo", "d1z"),
                new SignalParamDto("signal_b", "bar", "efg")),
            12);
    conn.commit();
    assertEquals(2, added);
  }

  @After
  public void cleanup() throws Exception {
    conn.prepareStatement("DELETE FROM maestro_signal_param").executeUpdate();
    conn.commit();
    conn.close();
  }

  @Test
  public void testAddSignalParams() throws Exception {
    var added =
        paramDao.addSignalParams(
            conn,
            List.of(
                new SignalParamDto("signal_a", "foo", "d1z"),
                new SignalParamDto("signal_b", "bar", "efg")),
            12);
    conn.commit();
    assertEquals(0, added);
  }

  @Test
  public void testMatchSignalDependency() {
    Long matched =
        paramDao.matchSignalDependency(
            new SignalMatchDto(
                "signal_a",
                List.of(
                    new SignalMatchDto.ParamMatchDto(
                        "foo", SignalParamValue.of(123), SignalOperator.EQUALS_TO))));
    assertEquals(12, matched.intValue());
  }

  @Test
  public void testNoMatchSignalDependency() {
    Long matched =
        paramDao.matchSignalDependency(
            new SignalMatchDto(
                "signal_a",
                List.of(
                    new SignalMatchDto.ParamMatchDto(
                        "foo", SignalParamValue.of(125), SignalOperator.GREATER_THAN))));
    assertNull(matched);

    matched =
        paramDao.matchSignalDependency(
            new SignalMatchDto(
                "signal_a",
                List.of(
                    new SignalMatchDto.ParamMatchDto(
                        "foo", SignalParamValue.of(125), SignalOperator.EQUALS_TO),
                    new SignalMatchDto.ParamMatchDto(
                        "bar", SignalParamValue.of(125), SignalOperator.EQUALS_TO))));
    assertNull(matched);
  }

  @Test
  public void testMatchSignal() throws Exception {
    Long matched =
        paramDao.matchSignal(
            conn,
            new SignalMatchDto(
                "signal_a",
                List.of(
                    new SignalMatchDto.ParamMatchDto(
                        "foo", SignalParamValue.of(125), SignalOperator.LESS_THAN))),
            11);
    assertEquals(12, matched.intValue());
  }

  @Test
  public void testNoMatchSignal() throws Exception {
    Long matched =
        paramDao.matchSignal(
            conn,
            new SignalMatchDto(
                "signal_a",
                List.of(
                    new SignalMatchDto.ParamMatchDto(
                        "foo", SignalParamValue.of(125), SignalOperator.LESS_THAN))),
            12);
    assertNull(matched);
  }
}
