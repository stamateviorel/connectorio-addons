package org.connectorio.addons.binding.ocpp.internal.server;

import eu.chargetime.ocpp.feature.profile.ServerCoreEventHandler;
import eu.chargetime.ocpp.model.Confirmation;
import eu.chargetime.ocpp.model.Request;
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
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

public class CoreEventHandlerWrapper implements ServerCoreEventHandler {

  private final Deque<ServerCoreEventHandler> handlers;
  /** Notified with the session UUID on every inbound OCPP message — feeds the server's liveness watchdog. */
  private final Consumer<UUID> onInbound;

  public CoreEventHandlerWrapper(Deque<ServerCoreEventHandler> handlers, Consumer<UUID> onInbound) {
    this.handlers = handlers;
    this.onInbound = onInbound;
  }

  @Override
  public AuthorizeConfirmation handleAuthorizeRequest(UUID sessionIndex, AuthorizeRequest request) {
    return process(sessionIndex, handler -> handler.handleAuthorizeRequest(sessionIndex, request));
  }

  @Override
  public BootNotificationConfirmation handleBootNotificationRequest(UUID sessionIndex, BootNotificationRequest request) {
    return process(sessionIndex, handler -> handler.handleBootNotificationRequest(sessionIndex, request));
  }

  @Override
  public DataTransferConfirmation handleDataTransferRequest(UUID sessionIndex, DataTransferRequest request) {
    return process(sessionIndex, handler -> handler.handleDataTransferRequest(sessionIndex, request));
  }

  @Override
  public HeartbeatConfirmation handleHeartbeatRequest(UUID sessionIndex, HeartbeatRequest request) {
    return process(sessionIndex, handler -> handler.handleHeartbeatRequest(sessionIndex, request));
  }

  @Override
  public MeterValuesConfirmation handleMeterValuesRequest(UUID sessionIndex, MeterValuesRequest request) {
    return process(sessionIndex, handler -> handler.handleMeterValuesRequest(sessionIndex, request));
  }

  @Override
  public StartTransactionConfirmation handleStartTransactionRequest(UUID sessionIndex, StartTransactionRequest request) {
    return process(sessionIndex, handler -> handler.handleStartTransactionRequest(sessionIndex, request));
  }

  @Override
  public StatusNotificationConfirmation handleStatusNotificationRequest(UUID sessionIndex, StatusNotificationRequest request) {
    return process(sessionIndex, handler -> handler.handleStatusNotificationRequest(sessionIndex, request));
  }

  @Override
  public StopTransactionConfirmation handleStopTransactionRequest(UUID sessionIndex, StopTransactionRequest request) {
    return process(sessionIndex, handler -> handler.handleStopTransactionRequest(sessionIndex, request));
  }

  private <T extends Request, C extends Confirmation> C process(UUID sessionIndex, Function<ServerCoreEventHandler, C> consumer) {
    // Any inbound OCPP message proves the session is alive at the application layer (WebSocket
    // ping/pong alone does not — a charger whose OCPP stack froze still pongs). Stamp it so the
    // server's liveness watchdog can reap a session that goes silent.
    onInbound.accept(sessionIndex);
    C confirmation = null;
    for (ServerCoreEventHandler handler : handlers) {
      C handlerConfirmation = consumer.apply(handler);
      if (confirmation == null && handlerConfirmation != null && handlerConfirmation.validate()) {
        confirmation = handlerConfirmation;
      }
    }
    return confirmation;
  }

}
