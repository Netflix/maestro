package com.netflix.maestro.signal.dao;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.MaestroDatabaseHelper;
import com.netflix.maestro.metrics.MaestroMetrics;
import javax.sql.DataSource;
import org.junit.BeforeClass;
import org.mockito.Mockito;

/**
 * Base test to set up db.
 *
 * @author jun-he
 */
abstract class MaestroBaseSignalDaoTest extends MaestroBaseTest {
  static final DatabaseConfiguration CONFIG = MaestroDatabaseHelper.getConfig();
  static final DataSource DATA_SOURCE = MaestroDatabaseHelper.getDataSource();
  static MaestroMetrics METRICS;

  @BeforeClass
  public static void init() {
    METRICS = Mockito.mock(MaestroMetrics.class);
  }
}
