package com.netflix.maestro.flow;

/**
 * Constants used by maestro flow engine.
 *
 * @author jun-he
 */
public final class Constants {
  private Constants() {}

  /** Graceful shutdown timeout config in seconds. */
  public static final long GRACEFUL_SHUTDOWN_TIMEOUT_IN_SECS = 60;

  /** Initial generation number value. */
  public static final long INITIAL_GENERATION_NUMBER = 1;

  /** Exit code to indicate an invalid ownership. */
  public static final int INVALID_OWNERSHIP_EXIT_CODE = 1;
}
