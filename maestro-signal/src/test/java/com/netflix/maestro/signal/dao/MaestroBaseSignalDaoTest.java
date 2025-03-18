package com.netflix.maestro.signal.dao;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.DatabaseSourceProvider;
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
  static DatabaseConfiguration CONFIG;
  static DataSource DATA_SOURCE;
  static MaestroMetrics METRICS;

  private static class MaestroDBTestConfiguration implements DatabaseConfiguration {
    @Override
    public String getJdbcUrl() {
      return "jdbc:tc:cockroach:v22.2.19:///maestro";
    }

    @Override
    public int getConnectionPoolMaxSize() {
      return 10;
    }

    @Override
    public int getConnectionPoolMinIdle() {
      return getConnectionPoolMaxSize();
    }
  }

  @BeforeClass
  public static void init() {
    CONFIG = new MaestroDBTestConfiguration();
    DATA_SOURCE = new DatabaseSourceProvider(CONFIG).get();
    METRICS = Mockito.mock(MaestroMetrics.class);
  }
}
