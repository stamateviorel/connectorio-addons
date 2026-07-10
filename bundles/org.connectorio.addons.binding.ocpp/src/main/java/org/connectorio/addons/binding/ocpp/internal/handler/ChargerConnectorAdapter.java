package org.connectorio.addons.binding.ocpp.internal.handler;

import eu.chargetime.ocpp.model.Request;
import eu.chargetime.ocpp.model.core.AuthorizationStatus;
import eu.chargetime.ocpp.model.core.IdTagInfo;
import eu.chargetime.ocpp.model.core.MeterValuesConfirmation;
import eu.chargetime.ocpp.model.core.MeterValuesRequest;
import eu.chargetime.ocpp.model.core.StartTransactionConfirmation;
import eu.chargetime.ocpp.model.core.StartTransactionRequest;
import eu.chargetime.ocpp.model.core.StatusNotificationConfirmation;
import eu.chargetime.ocpp.model.core.StatusNotificationRequest;
import eu.chargetime.ocpp.model.core.StopTransactionConfirmation;
import eu.chargetime.ocpp.model.core.StopTransactionRequest;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import org.connectorio.addons.binding.ocpp.internal.OcppRequestListener;
import org.connectorio.addons.binding.ocpp.internal.server.listener.MeterValuesHandler;
import org.connectorio.addons.binding.ocpp.internal.server.listener.StatusNotificationHandler;
import org.connectorio.addons.binding.ocpp.internal.server.listener.TransactionHandler;

public class ChargerConnectorAdapter implements StatusNotificationHandler, MeterValuesHandler,
  TransactionHandler {

  private final Map<Integer, ConnectorThingHandler> handlers = new ConcurrentHashMap<>();
  private final Map<Integer, Integer> transactionMap = new ConcurrentHashMap<>();
  // Charger-wide transaction-id sequence shared with every connector so their ids never collide;
  // StopTransaction carries no connectorId, so the id is the only key back to the right connector.
  private final AtomicInteger transactionSequence = new AtomicInteger(1);
  private final OcppRequestListener<Request> listener;
  // idTag policy for StartTransaction.req, wired from the server bridge's `tags` whitelist.
  // Default accepts everything — matches an empty whitelist and standalone/test construction.
  private volatile Predicate<String> tagValidator = tag -> true;

  public ChargerConnectorAdapter(OcppRequestListener<Request> listener) {
    this.listener = listener;
  }

  public void setTagValidator(Predicate<String> tagValidator) {
    if (tagValidator != null) {
      this.tagValidator = tagValidator;
    }
  }

  public void addConnector(int connector, ConnectorThingHandler handler) {
    handler.setTransactionSequence(transactionSequence);
    handlers.put(connector, handler);
    // A connector that was charging when the binding stopped restored its transaction id from
    // persisted state in initialize(). Re-register that mapping so the charger's StopTransaction
    // (which carries no connectorId) still routes back here after the restart, and advance the shared
    // id sequence past it so a freshly started transaction can never reuse the still-open id.
    Integer restored = handler.getCurrentTransactionId();
    if (restored != null) {
      transactionMap.put(connector, restored);
      transactionSequence.updateAndGet(current -> Math.max(current, restored + 1));
    }
  }

  public void removeConnector(int connector) {
    handlers.remove(connector);
  }

  @Override
  public StatusNotificationConfirmation handleStatusNotification(StatusNotificationRequest request) {
    listener.onRequest(request);
    if (!handlers.containsKey(request.getConnectorId())) {
      // Charger-level (connectorId 0) or a connector with no Thing — ACK so the OCPP layer does
      // not answer the charge point with NotSupported.
      return new StatusNotificationConfirmation();
    }
    return handle(handler -> handler.handleStatusNotification(request), request.getConnectorId());
  }

  @Override
  public MeterValuesConfirmation handleMeterValues(MeterValuesRequest request) {
    listener.onRequest(request);
    if (!handlers.containsKey(request.getConnectorId())) {
      // Charger-level (connectorId 0, e.g. idle clock-aligned MeterValues) or a connector with no
      // Thing — ACK with an empty confirmation rather than letting the library reply NotSupported.
      return new MeterValuesConfirmation();
    }
    return handle(handler -> handler.handleMeterValues(request), request.getConnectorId());
  }

  @Override
  public StartTransactionConfirmation handleStartTransaction(StartTransactionRequest request) {
    listener.onRequest(request);

    // OCPP requires the CSMS to verify the idTag presented here too — a charger using local
    // pre-authorization (or FreeMode) starts the transaction without a preceding Authorize.req,
    // making this the only authorization checkpoint. Checked before routing so a refused start
    // produces no connector side effects (no transaction id adopted, no charging state flipped).
    // The response still carries a real unique transaction id: the id is schema-required, and the
    // charger references it in its follow-up StopTransaction when StopTransactionOnInvalidId ends
    // the session.
    if (!tagValidator.test(request.getIdTag())) {
      return new StartTransactionConfirmation(new IdTagInfo(AuthorizationStatus.Invalid),
          transactionSequence.getAndIncrement());
    }

    if (!handlers.containsKey(request.getConnectorId())) {
      // Connector with no Thing — OCPP still requires a StartTransaction.conf (a CallError here
      // strands the charger's locally-running transaction, the exact wedge the StopTransaction
      // tolerance fixes on the other end). Accept with a real unique id; left out of
      // transactionMap on purpose — its StopTransaction falls through to the generic ACK below.
      return new StartTransactionConfirmation(new IdTagInfo(AuthorizationStatus.Accepted),
          transactionSequence.getAndIncrement());
    }

    StartTransactionConfirmation confirmation = handle(handler -> handler.handleStartTransaction(request), request.getConnectorId());
    if (confirmation != null) {
      transactionMap.put(request.getConnectorId(), confirmation.getTransactionId());
    }
    return confirmation;
  }

  @Override
  public StopTransactionConfirmation handleStopTransaction(StopTransactionRequest request) {
    listener.onRequest(request);
    Integer connectorId = null;
    for (Entry<Integer, Integer> entry : transactionMap.entrySet()) {
      if (entry.getValue().equals(request.getTransactionId())) {
        connectorId = entry.getKey();
        break;
      }
    }

    if (connectorId != null) {
      StopTransactionConfirmation confirmation =
          handle(handler -> handler.handleStopTransaction(request), connectorId);
      if (confirmation != null) {
        return confirmation;
      }
      // Tracked connector whose Thing handler is gone (disposed between start and stop) — fall
      // through to the generic ACK rather than letting the library answer NotSupported.
    }
    // Unknown transaction — no StartTransaction was tracked for this id. Common with free-charging
    // (FreeMode): the charger runs a local transaction and sends StopTransaction at session end without
    // a StartTransaction we ever saw. OCPP requires the CSMS to ACK StopTransaction regardless; reply
    // with an empty confirmation instead of returning null, which makes the library send a NotSupported
    // CallError — leaving the charger retrying or holding a dangling transaction (which can then block
    // the next StartTransaction).
    return new StopTransactionConfirmation();
  }

  private <C> C handle(Function<ConnectorThingHandler, C> handler, int connector) {
    if (handlers.containsKey(connector)) {
      ConnectorThingHandler connectorHandler = handlers.get(connector);
      return handler.apply(connectorHandler);
    }

    return null;
  }

}
