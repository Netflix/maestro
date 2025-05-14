package com.netflix.maestro.server.controllers;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.SignalCreateRequest;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Maestro signal related REST API.
 *
 * @author jun-he
 */
@Tag(name = "/api/v3/signals", description = "Maestro Signal APIs")
@RestController
@RequestMapping(
    value = "/api/v3/signals",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class SignalController {

  private final MaestroSignalBrokerDao brokerDao;

  @Autowired
  public SignalController(MaestroSignalBrokerDao brokerDao) {
    this.brokerDao = brokerDao;
  }

  @PostMapping(value = "", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Announce a signal")
  public SignalInstance announceSignal(@Valid @NotNull @RequestBody SignalCreateRequest request)
      throws Throwable {
    SignalInstance instance = brokerDao.addSignal(request);
    if (instance.getDetails() != null) {
      throw instance.getDetails().getCause();
    }
    return instance;
  }

  @GetMapping(value = "/{signalName}/instances/{seq}", consumes = MediaType.ALL_VALUE)
  @Operation(
      summary =
          "Get a signal instance for a given signal name and a sequence info (e.g. exact seq id or 'latest')")
  public SignalInstance getSignalInstance(
      @Valid @NotNull @PathVariable("signalName") String signalName,
      @Valid @NotNull @PathVariable("seq") String seq) {
    if (Constants.LATEST_INSTANCE_RUN.equalsIgnoreCase(seq)) {
      return brokerDao.getLatestSignalInstance(signalName);
    } else {
      return brokerDao.getSignalInstance(signalName, Long.parseLong(seq));
    }
  }
}
