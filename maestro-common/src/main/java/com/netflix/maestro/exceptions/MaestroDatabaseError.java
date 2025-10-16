package com.netflix.maestro.exceptions;

import com.netflix.maestro.models.error.Details;
import lombok.Getter;

@Getter
@SuppressWarnings("PMD.NonSerializableClass")
public class MaestroDatabaseError extends MaestroRuntimeException {
  private static final long serialVersionUID = 7334668492533395123L;

  private final Details details;

  /**
   * Constructor with error message and details.
   *
   * @param cause cause exception
   */
  public MaestroDatabaseError(Throwable cause, String msg) {
    super(Code.INTERNAL_ERROR, msg, cause);
    this.details = Details.create(cause, false, msg);
  }
}
