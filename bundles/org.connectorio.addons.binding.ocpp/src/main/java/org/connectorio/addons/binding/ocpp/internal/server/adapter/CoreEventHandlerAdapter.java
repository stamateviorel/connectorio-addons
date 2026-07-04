package org.connectorio.addons.binding.ocpp.internal.server.adapter;

import eu.chargetime.ocpp.feature.profile.ServerCoreEventHandler;
import eu.chargetime.ocpp.model.core.AuthorizeConfirmation;
import eu.chargetime.ocpp.model.core.AuthorizeRequest;
import eu.chargetime.ocpp.model.core.BootNotificationConfirmation;
import eu.chargetime.ocpp.model.core.BootNotificationRequest;
import eu.chargetime.ocpp.model.core.DataTransferConfirmation;
import eu.chargetime.ocpp.model.core.DataTransferRequest;
import eu.chargetime.ocpp.model.core.HeartbeatConfirmation;
import eu.chargetime.ocpp.model.core.HeartbeatRequest;
import eu.chargetime.ocpp.model.core.MeterValuesConfirmation;
import eu.chargetime.ocpp.model.core.MeterValuesRequest;
import eu.chargetime.ocpp.model.core.StartTransactionConfirmation;
import eu.chargetime.ocpp.model.core.StartTransactionRequest;
import eu.chargetime.ocpp.model.core.StatusNotificationConfirmation;
import eu.chargetime.ocpp.model.core.StatusNotificationRequest;
import eu.chargetime.ocpp.model.core.StopTransactionConfirmation;
import eu.chargetime.ocpp.model.core.StopTransactionRequest;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;

public class CoreEventHandlerAdapter implements ServerCoreEventHandler {

  private final Set<String> configuredChargers = ConcurrentHashMap.newKeySet();

  /**
   * Delay before the once-per-charger boot-time configuration burst is sent. A charger that just
   * (re)booted announces itself with a BootNotification before its OCPP stack is ready to answer
   * ChangeConfiguration; firing the burst immediately gets the first CALL timed out, which closes the
   * freshly established session and delays charging. Sending it via {@code OcppSender.sendAfter} a few
   * seconds later lets the charger settle. These config values are persisted charger-side anyway, so a
   * short delay (or even a missed re-send) is harmless.
   */
  protected static final long CONFIG_SETTLE_SECONDS = 10;

  /**
   * Returns {@code true} the first time a given charger is seen and {@code false} thereafter.
   * Config adapters push their ChangeConfiguration once per charger rather than on every
   * BootNotification — a charger that re-announces repeatedly would otherwise be flooded with the
   * full config burst each time, CALLs a sluggish charger never answers.
   */
  protected boolean firstBootForConfig(ChargerReference reference) {
    return reference != null && reference.getSerial() != null
        && configuredChargers.add(reference.getSerial());
  }

  @Override
  public AuthorizeConfirmation handleAuthorizeRequest(UUID sessionIndex, AuthorizeRequest request) {
    return null;
  }

  @Override
  public BootNotificationConfirmation handleBootNotificationRequest(UUID sessionIndex, BootNotificationRequest request) {
    return null;
  }

  @Override
  public DataTransferConfirmation handleDataTransferRequest(UUID sessionIndex, DataTransferRequest request) {
    return null;
  }

  @Override
  public HeartbeatConfirmation handleHeartbeatRequest(UUID sessionIndex, HeartbeatRequest request) {
    return null;
  }

  @Override
  public MeterValuesConfirmation handleMeterValuesRequest(UUID sessionIndex, MeterValuesRequest request) {
    return null;
  }

  @Override
  public StartTransactionConfirmation handleStartTransactionRequest(UUID sessionIndex, StartTransactionRequest request) {
    return null;
  }

  @Override
  public StatusNotificationConfirmation handleStatusNotificationRequest(UUID sessionIndex, StatusNotificationRequest request) {
    return null;
  }

  @Override
  public StopTransactionConfirmation handleStopTransactionRequest(UUID sessionIndex, StopTransactionRequest request) {
    return null;
  }
}
