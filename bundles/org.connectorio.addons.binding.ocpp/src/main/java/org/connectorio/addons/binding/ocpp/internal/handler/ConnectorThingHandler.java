package org.connectorio.addons.binding.ocpp.internal.handler;

import eu.chargetime.ocpp.model.Request;
import eu.chargetime.ocpp.model.core.AuthorizationStatus;
import eu.chargetime.ocpp.model.core.AvailabilityType;
import eu.chargetime.ocpp.model.core.ChangeAvailabilityRequest;
import eu.chargetime.ocpp.model.core.ChargePointStatus;
import eu.chargetime.ocpp.model.core.IdTagInfo;
import eu.chargetime.ocpp.model.core.MeterValue;
import eu.chargetime.ocpp.model.core.MeterValuesConfirmation;
import eu.chargetime.ocpp.model.core.MeterValuesRequest;
import eu.chargetime.ocpp.model.core.SampledValue;
import eu.chargetime.ocpp.model.core.StartTransactionConfirmation;
import eu.chargetime.ocpp.model.core.StartTransactionRequest;
import eu.chargetime.ocpp.model.core.StatusNotificationConfirmation;
import eu.chargetime.ocpp.model.core.StatusNotificationRequest;
import eu.chargetime.ocpp.model.core.StopTransactionConfirmation;
import eu.chargetime.ocpp.model.core.StopTransactionRequest;
import eu.chargetime.ocpp.model.core.UnlockConnectorRequest;
import eu.chargetime.ocpp.model.core.ValueFormat;
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequest;
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequestType;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.measure.Quantity;
import org.connectorio.addons.binding.handler.GenericThingHandlerBase;
import org.connectorio.addons.binding.ocpp.OcppBindingConstants;
import org.connectorio.addons.binding.ocpp.internal.config.ConnectorConfig;
import org.connectorio.addons.binding.ocpp.internal.server.OcppMeasurementMapping;
import org.connectorio.addons.binding.ocpp.internal.server.PhantomCycleDetector;
import org.connectorio.addons.binding.ocpp.internal.server.listener.MeterValuesHandler;
import org.connectorio.addons.binding.ocpp.internal.server.listener.StatusNotificationHandler;
import org.connectorio.addons.binding.ocpp.internal.server.listener.TransactionHandler;
import org.connectorio.addons.binding.ocpp.internal.OcppSender;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;
import org.openhab.core.library.types.DateTimeType;
import org.openhab.core.library.types.DecimalType;
import org.openhab.core.library.types.QuantityType;
import org.openhab.core.library.types.StringType;
import org.openhab.core.library.unit.Units;
import org.openhab.core.thing.Bridge;
import org.openhab.core.thing.ChannelUID;
import org.openhab.core.thing.Thing;
import org.openhab.core.thing.ThingStatus;
import org.openhab.core.thing.UID;
import org.openhab.core.thing.binding.ThingHandler;
import org.openhab.core.thing.binding.ThingHandlerCallback;
import org.openhab.core.library.types.OnOffType;
import org.openhab.core.types.Command;
import org.openhab.core.types.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.units.indriya.quantity.Quantities;

public class ConnectorThingHandler extends GenericThingHandlerBase<ServerBridgeHandler, ConnectorConfig> implements
  StatusNotificationHandler, TransactionHandler, MeterValuesHandler, ConnectorCommandContext {

  /**
   * Phantom-cycle detection window/threshold/reset-delay. See {@link PhantomCycleDetector}.
   */
  private static final long PHANTOM_WINDOW_MS = 60_000L;
  private static final int PHANTOM_CYCLE_THRESHOLD = 2;
  private static final long DEFAULT_PROFILE_MIN_INTERVAL_MS = 500L;
  private static final long WATCHDOG_TICK_SECONDS = 5L;
  private static final long AVAILABILITY_RESTORE_DELAY_MS = 2_000L;

  private final AtomicInteger transactionId = new AtomicInteger();
  private final Logger logger = LoggerFactory.getLogger(ConnectorThingHandler.class);

  private final PhantomCycleDetector phantomDetector =
      new PhantomCycleDetector(PHANTOM_WINDOW_MS, PHANTOM_CYCLE_THRESHOLD);
  private final org.connectorio.addons.binding.ocpp.internal.server.StuckStateWatchdog watchdog =
      new org.connectorio.addons.binding.ocpp.internal.server.StuckStateWatchdog();
  private ScheduledFuture<?> watchdogFuture;

  private OcppSender ocppSender;
  private String chargerSerialNumber;
  private Integer currentTransactionId;
  private String remoteStartTag;
  private Integer connectorId;

  private final ChargeLimitCommandHandler chargeLimitHandler;
  private final ChargingCommandHandler chargingHandler;

  public ConnectorThingHandler(Thing thing) {
    this(thing, new ChargeLimitCommandHandler(), new ChargingCommandHandler());
  }

  ConnectorThingHandler(Thing thing, ChargeLimitCommandHandler chargeLimitHandler, ChargingCommandHandler chargingHandler) {
    super(thing);
    this.chargeLimitHandler = chargeLimitHandler;
    this.chargingHandler = chargingHandler;
  }

  protected void setOcppSender(OcppSender sender, String chargerSerial) {
    this.ocppSender = sender;
    this.chargerSerialNumber = chargerSerial;
  }

  @Override
  public OcppSender getOcppSender() {
    return ocppSender;
  }

  @Override
  public String getChargerSerialNumber() {
    return chargerSerialNumber;
  }

  @Override
  public String getRemoteStartTag() {
    return remoteStartTag;
  }

  @Override
  public Integer getCurrentTransactionId() {
    return currentTransactionId;
  }

  @Override
  public Integer getConnectorId() {
    return connectorId;
  }

  @Override
  public java.util.concurrent.ScheduledExecutorService getScheduler() {
    return scheduler;
  }

  @Override
  public long getProfileMinIntervalMs() {
    Object value = getThing().getConfiguration().get("profileMinIntervalMs");
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value instanceof String) {
      try {
        return Long.parseLong(((String) value).trim());
      } catch (NumberFormatException e) {
        // fall through to default
      }
    }
    return DEFAULT_PROFILE_MIN_INTERVAL_MS;
  }

  @Override
  public void initialize() {
    Optional<ConnectorConfig> config = getThingConfig();
    if (config.isPresent()) {
      remoteStartTag = config.get().remoteStartTag;
      if (remoteStartTag == null || remoteStartTag.trim().isEmpty()) {
        remoteStartTag = ConnectorConfig.DEFAULT_REMOTE_START_TAG;
      }
      connectorId = config.get().connectorId;
    } else {
      remoteStartTag = ConnectorConfig.DEFAULT_REMOTE_START_TAG;
    }
    watchdogFuture = scheduler.scheduleWithFixedDelay(this::runWatchdog,
        WATCHDOG_TICK_SECONDS, WATCHDOG_TICK_SECONDS, TimeUnit.SECONDS);
    updateStatus(ThingStatus.ONLINE);
  }

  @Override
  public void dispose() {
    if (watchdogFuture != null) {
      watchdogFuture.cancel(true);
      watchdogFuture = null;
    }
    super.dispose();
  }

  @Override
  public void handleCommand(ChannelUID channelUID, Command command) {
    String channelId = channelUID.getId();
    if (OcppBindingConstants.CHARGE_LIMIT.getAsString().equals(channelId)) {
      chargeLimitHandler.handle(command, this);
    } else if (OcppBindingConstants.CHARGING.getAsString().equals(channelId)) {
      chargingHandler.handle(command, this);
    } else if (OcppBindingConstants.PAUSE.getAsString().equals(channelId)) {
      if (command instanceof OnOffType) {
        if (command == OnOffType.ON) {
          chargeLimitHandler.pause(this);
        } else {
          chargeLimitHandler.resume(this);
        }
      }
    }
  }

  @Override
  public MeterValuesConfirmation handleMeterValues(MeterValuesRequest request) {
    ThingHandlerCallback callback = getCallback();

    // push transaction id
    Integer transactionId = request.getTransactionId();
    // transactionId is null outside of an active charge transaction
    if (transactionId != null) {
      currentTransactionId = transactionId;
      callback.stateUpdated(new ChannelUID(getThing().getUID(), "transactionId"), new DecimalType(transactionId));
    }

    for (MeterValue value : request.getMeterValue()) {
      ZonedDateTime timestamp = value.getTimestamp();
      // update timestamp for further channel updates
      callback.stateUpdated(new ChannelUID(getThing().getUID(), "timestamp"), new DateTimeType(timestamp));

      logger.debug("Received samples for transaction {}: {}", transactionId, value);

      for (SampledValue sample : value.getSampledValue()) {
        if (!ValueFormat.Raw.equals(sample.getFormat())) {
          // unsupported case with encrypted measurements
          continue;
        }

        try {
          Double measurement = Double.valueOf(sample.getValue());
          for (UID ref : OcppMeasurementMapping.channelsFor(sample)) {
            ChannelUID uid = new ChannelUID(getThing().getUID(), ref.getAsString());
            State state = parse(measurement, uid, sample);
            getCallback().stateUpdated(uid, state);
          }
        } catch (NumberFormatException e) {
          logger.debug("Could not parse value of measurement {}", sample, e);
        }
      }
    }

    return new MeterValuesConfirmation();
  }

  @Override
  public StatusNotificationConfirmation handleStatusNotification(StatusNotificationRequest request) {
    ChargePointStatus status = request.getStatus();

    StringType val = new StringType(status.name());
    getCallback().stateUpdated(new ChannelUID(getThing().getUID(), "chargePointStatus"), val);

    trackPhantomCycle(status);
    watchdog.onStatus(status, System.currentTimeMillis());

    getCallback().stateUpdated(
        new ChannelUID(getThing().getUID(), OcppBindingConstants.CABLE_CONNECTED.getAsString()),
        OnOffType.from(isCableConnected(status)));

    return new StatusNotificationConfirmation();
  }

  private void trackPhantomCycle(ChargePointStatus status) {
    if (phantomDetector.record(status, System.currentTimeMillis())) {
      logger.warn("Connector {} returned to Available {}+ times within {}s without charging —"
              + " resetting connector state via ChangeAvailability(Inoperative→Operative)",
          getThing().getUID(), PHANTOM_CYCLE_THRESHOLD, PHANTOM_WINDOW_MS / 1000);
      Integer connector = resolveConnectorId();
      if (ocppSender != null && chargerSerialNumber != null && connector != null) {
        cycleAvailability(new ChargerReference(chargerSerialNumber), connector);
      }
    }
  }

  private void runWatchdog() {
    org.connectorio.addons.binding.ocpp.internal.server.StuckStateWatchdog.Action action =
        watchdog.evaluate(System.currentTimeMillis());
    if (action == org.connectorio.addons.binding.ocpp.internal.server.StuckStateWatchdog.Action.NONE) {
      return;
    }
    Integer connector = resolveConnectorId();
    if (ocppSender == null || chargerSerialNumber == null || connector == null) {
      return;
    }
    ChargerReference reference = new ChargerReference(chargerSerialNumber);
    switch (action) {
      case TRIGGER_STATUS:
        logger.info("Connector {} stuck — nudging with TriggerMessage(StatusNotification)", getThing().getUID());
        TriggerMessageRequest trigger = new TriggerMessageRequest(TriggerMessageRequestType.StatusNotification);
        trigger.setConnectorId(connector);
        sendRecovery(reference, trigger, "TriggerMessage(StatusNotification)");
        break;
      case CHANGE_AVAILABILITY:
        logger.warn("Connector {} stuck — cycling ChangeAvailability(Inoperative->Operative)", getThing().getUID());
        cycleAvailability(reference, connector);
        break;
      case UNLOCK:
        logger.error("Connector {} still stuck — sending UnlockConnector; physical intervention may be required",
            getThing().getUID());
        sendRecovery(reference, new UnlockConnectorRequest(connector), "UnlockConnector");
        break;
      default:
        break;
    }
  }

  private void cycleAvailability(ChargerReference reference, Integer connector) {
    ocppSender.send(reference, new ChangeAvailabilityRequest(connector, AvailabilityType.Inoperative))
        .whenComplete((confirmation, ex) -> {
          if (ex != null) {
            logger.warn("ChangeAvailability(Inoperative) for {} failed: {}", getThing().getUID(), ex.getMessage());
            return;
          }
          scheduler.schedule(() -> sendRecovery(reference,
              new ChangeAvailabilityRequest(connector, AvailabilityType.Operative),
              "ChangeAvailability(Operative)"), AVAILABILITY_RESTORE_DELAY_MS, TimeUnit.MILLISECONDS);
        });
  }

  private void sendRecovery(ChargerReference reference, Request request, String label) {
    ocppSender.send(reference, request).whenComplete((confirmation, ex) -> {
      if (ex != null) {
        logger.warn("{} for {} failed: {}", label, getThing().getUID(), ex.getMessage());
      } else {
        logger.debug("{} for {}: {}", label, getThing().getUID(), confirmation);
      }
    });
  }

  private Integer resolveConnectorId() {
    Object value = getThing().getConfiguration().get("connectorId");
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    if (value instanceof String) {
      try {
        return Integer.parseInt(((String) value).trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Derive cable presence from the OCPP connector status. A cable is considered connected from the
   * moment the EV is plugged in (Preparing) through the whole session up to teardown (Finishing);
   * Available/Unavailable/Faulted/Reserved mean nothing is plugged in.
   */
  private static boolean isCableConnected(ChargePointStatus status) {
    switch (status) {
      case Preparing:
      case Charging:
      case SuspendedEV:
      case SuspendedEVSE:
      case Finishing:
        return true;
      default:
        return false;
    }
  }

  @Override
  public StartTransactionConfirmation handleStartTransaction(StartTransactionRequest request) {
    String tag = request.getIdTag();

    ThingHandlerCallback callback = getCallback();
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "idTag"), new StringType(tag));
    callback.stateUpdated(new ChannelUID(getThing().getUID(), OcppBindingConstants.CHARGING.getAsString()), OnOffType.ON);
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "timestampStart"), new DateTimeType(request.getTimestamp()));
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "meterStart"), new QuantityType<>(request.getMeterStart(), Units.WATT_HOUR));

    IdTagInfo tagInfo = new IdTagInfo(AuthorizationStatus.Accepted);
    return new StartTransactionConfirmation(tagInfo, generateId());
  }

  @Override
  public StopTransactionConfirmation handleStopTransaction(StopTransactionRequest request) {
    String tag = request.getIdTag();

    Integer txId = request.getTransactionId();
    if (transactionId.get() != txId + 1) {
      return new StopTransactionConfirmation();
    }

    currentTransactionId = null;
    ThingHandlerCallback callback = getCallback();
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "idTag"), new StringType(tag));
    callback.stateUpdated(new ChannelUID(getThing().getUID(), OcppBindingConstants.CHARGING.getAsString()), OnOffType.OFF);
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "timestampStop"), new DateTimeType(request.getTimestamp()));
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "meterStop"), new QuantityType<>(request.getMeterStop(), Units.WATT_HOUR));

    return new StopTransactionConfirmation();
  }

  private static State parse(Double measurement, ChannelUID uid, SampledValue sample) {
    String unit = sample.getUnit();
    if (unit != null) {
      // Normalize unit names that don't match JSR-385 format
      switch (unit) {
        case "Celsius": unit = "°C"; break;
        case "Fahrenheit": unit = "°F"; break;
        default: break;
      }
      Quantity<?> quantity = Quantities.getQuantity("1 " + unit);
      return new QuantityType<>(measurement, quantity.getUnit());
    }

    // default assumed from specs, when unit is not specified it fall backs to "Wh"
    return new QuantityType<>(measurement, Units.WATT_HOUR);
  }

  private int generateId() {
    int transaction = transactionId.getAndIncrement();
    if (transaction == Integer.MAX_VALUE) {
      transactionId.set(0);
    }
    return transaction;
  }

}
