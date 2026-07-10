package org.connectorio.addons.binding.ocpp.internal.handler;

import eu.chargetime.ocpp.model.Request;
import eu.chargetime.ocpp.model.core.AuthorizationStatus;
import eu.chargetime.ocpp.model.core.AvailabilityType;
import eu.chargetime.ocpp.model.core.ChangeAvailabilityRequest;
import eu.chargetime.ocpp.model.core.ChangeConfigurationRequest;
import eu.chargetime.ocpp.model.core.ChargePointStatus;
import eu.chargetime.ocpp.model.core.GetConfigurationConfirmation;
import eu.chargetime.ocpp.model.core.GetConfigurationRequest;
import eu.chargetime.ocpp.model.core.IdTagInfo;
import eu.chargetime.ocpp.model.core.KeyValueType;
import eu.chargetime.ocpp.model.core.MeterValue;
import eu.chargetime.ocpp.model.core.MeterValuesConfirmation;
import eu.chargetime.ocpp.model.core.MeterValuesRequest;
import eu.chargetime.ocpp.model.core.SampledValue;
import eu.chargetime.ocpp.model.core.StartTransactionConfirmation;
import eu.chargetime.ocpp.model.core.StartTransactionRequest;
import eu.chargetime.ocpp.model.core.StatusNotificationConfirmation;
import eu.chargetime.ocpp.model.core.StatusNotificationRequest;
import eu.chargetime.ocpp.model.core.ResetRequest;
import eu.chargetime.ocpp.model.core.ResetType;
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
import org.openhab.core.types.RefreshType;
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
  private static final long DEFAULT_METER_POLL_SECONDS = 30L;

  /**
   * Thing property under which the active transaction id is persisted. Thing properties live in the
   * JSONDB and survive a binding reload / openHAB restart, so a connector that was charging when the
   * binding stopped recovers its transaction id on startup. Without it, the charger's StopTransaction
   * on unplug (which carries no connectorId) could not be routed back to its connector, leaving the
   * connector stuck in Finishing/Charging — and a dangling transaction can block the next session.
   */
  private static final String PROPERTY_ACTIVE_TRANSACTION = "activeTransactionId";

  // Transaction-id sequence. Replaced with a charger-wide shared sequence (see setTransactionSequence)
  // so connectors on the same charge point never issue colliding ids — StopTransaction carries only a
  // transactionId, no connectorId, so colliding ids cannot be routed back to the right connector.
  private AtomicInteger transactionId = new AtomicInteger(1);
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
  private String hardwareMaxCurrentKey;

  // MeterValues poll (TriggerMessage fallback). Chargers push MeterValues only during an active
  // transaction; a charger delivering power without an OCPP transaction never pushes, so power/energy
  // channels stay at zero. lastStatus/lastMeterValuesMs let runWatchdog pull a fresh sample when stale.
  private volatile ChargePointStatus lastStatus;
  private volatile long lastMeterValuesMs;
  private long lastMeterPollMs;
  private long meterPollIntervalMs = DEFAULT_METER_POLL_SECONDS * 1000L;

  // Status-freshness re-confirm. A StatusNotification dropped during a session close/reconnect leaves
  // the connector frozen in a stale "busy" status (e.g. a SuspendedEV the charger has since left) that
  // never refreshes, because a charger only re-reports on a status CHANGE. While a cable is believed
  // connected but neither a StatusNotification nor a MeterValues sample has arrived for this long,
  // re-pull this connector's status with a per-connector TriggerMessage(StatusNotification).
  private static final long STATUS_RECONFIRM_AFTER_MS = 300_000L;
  private volatile long lastStatusNotificationMs;
  private long lastStatusReconfirmMs;

  // The binding owns restoring a connector it made Inoperative (stuck-recovery cycle). If that Operative
  // restore is lost on a dropped session it must be retried — otherwise the connector strands Unavailable
  // indefinitely (it sat that way ~12 days). While set, the watchdog re-asserts Operative until the CALL
  // is accepted. Only set for binding-initiated cycles and cleared the moment the user takes manual
  // control of availability, so a deliberately disabled connector is never overridden.
  private volatile boolean operativeRestorePending;
  private long lastOperativeRetryMs;
  private static final long OPERATIVE_RETRY_INTERVAL_MS = 30_000L;

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

  /**
   * Share one transaction-id sequence across all connectors of a charge point so the ids stay unique
   * per charger. Called by {@link ChargerConnectorAdapter} when the connector is registered.
   */
  void setTransactionSequence(AtomicInteger sequence) {
    this.transactionId = sequence;
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

  /**
   * Set the active transaction id and persist it so it survives a binding/openHAB restart. Pass
   * {@code null} when the transaction ends to clear the persisted value.
   */
  private void setCurrentTransactionId(Integer id) {
    this.currentTransactionId = id;
    updateProperty(PROPERTY_ACTIVE_TRANSACTION, id == null ? null : id.toString());
  }

  @Override
  public Integer getConnectorId() {
    return connectorId;
  }

  @Override
  public boolean isForceTxDefaultProfile() {
    return getThingConfig().map(config -> config.forceTxDefaultProfile).orElse(false);
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
      hardwareMaxCurrentKey = config.get().hardwareMaxCurrentKey;
      Integer pollSeconds = config.get().meterValuesPollSeconds;
      meterPollIntervalMs = (pollSeconds != null && pollSeconds > 0) ? pollSeconds * 1000L : 0L;
    } else {
      remoteStartTag = ConnectorConfig.DEFAULT_REMOTE_START_TAG;
    }
    // Recover an in-flight transaction persisted before the last stop/restart so a StopTransaction
    // arriving after the restart still routes to this connector (see PROPERTY_ACTIVE_TRANSACTION).
    String persisted = getThing().getProperties().get(PROPERTY_ACTIVE_TRANSACTION);
    if (persisted != null && !persisted.trim().isEmpty()) {
      try {
        currentTransactionId = Integer.valueOf(persisted.trim());
        logger.info("Restored active transaction {} for {} from persisted state.", currentTransactionId,
            getThing().getUID());
      } catch (NumberFormatException e) {
        logger.warn("Ignoring malformed persisted transaction id '{}' for {}", persisted, getThing().getUID());
      }
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
    } else if (OcppBindingConstants.RESET.getAsString().equals(channelId)) {
      if (command == OnOffType.ON) {
        sendReset();
      }
    } else if (OcppBindingConstants.LOCK.getAsString().equals(channelId)) {
      if (command == OnOffType.ON) {
        sendUnlock();
      }
    } else if (OcppBindingConstants.AVAILABILITY.getAsString().equals(channelId)) {
      if (command instanceof OnOffType) {
        operativeRestorePending = false; // user is taking manual control — don't fight their choice
        sendAvailability(command == OnOffType.ON ? AvailabilityType.Operative : AvailabilityType.Inoperative);
      }
    } else if (OcppBindingConstants.HARDWARE_MAX_CURRENT.getAsString().equals(channelId)) {
      if (command instanceof RefreshType) {
        readHardwareMaxCurrent();
      } else if (command instanceof DecimalType) {
        writeHardwareMaxCurrent(((DecimalType) command).intValue());
      } else if (command instanceof QuantityType) {
        writeHardwareMaxCurrent(((QuantityType<?>) command).intValue());
      }
    }
  }

  private void sendReset() {
    if (ocppSender == null || chargerSerialNumber == null) {
      return;
    }
    ChargerReference reference = new ChargerReference(chargerSerialNumber);
    ocppSender.send(reference, new ResetRequest(ResetType.Soft)).whenComplete((confirmation, ex) -> {
      if (ex != null) {
        logger.warn("Reset(Soft) for {} failed: {}", getThing().getUID(), ex.getMessage());
      } else {
        logger.info("Reset(Soft) for {}: {}", getThing().getUID(), confirmation);
      }
      getCallback().stateUpdated(
          new ChannelUID(getThing().getUID(), OcppBindingConstants.RESET.getAsString()), OnOffType.OFF);
    });
  }

  /**
   * Send ChangeAvailability for this connector. ON → Operative, OFF → Inoperative. Lets a connector
   * the charger is holding Inoperative (e.g. disabled in the charger UI, or a stuck-recovery
   * Inoperative whose Operative restore failed on a dropped session and was never retried) be brought
   * back from openHAB without touching the charger's web UI.
   */
  private void sendAvailability(AvailabilityType type) {
    if (ocppSender == null || chargerSerialNumber == null || connectorId == null) {
      return;
    }
    ChargerReference reference = new ChargerReference(chargerSerialNumber);
    ocppSender.send(reference, new ChangeAvailabilityRequest(connectorId, type)).whenComplete((confirmation, ex) -> {
      if (ex != null) {
        logger.warn("ChangeAvailability({}) for {} failed: {}", type, getThing().getUID(), ex.getMessage());
      } else {
        logger.info("ChangeAvailability({}) for {}: {}", type, getThing().getUID(), confirmation);
      }
    });
  }

  private void sendUnlock() {
    if (ocppSender == null || chargerSerialNumber == null || connectorId == null) {
      return;
    }
    ChargerReference reference = new ChargerReference(chargerSerialNumber);
    ocppSender.send(reference, new UnlockConnectorRequest(connectorId)).whenComplete((confirmation, ex) -> {
      if (ex != null) {
        logger.warn("UnlockConnector for {} failed: {}", getThing().getUID(), ex.getMessage());
      } else {
        logger.info("UnlockConnector for {}: {}", getThing().getUID(), confirmation);
      }
      getCallback().stateUpdated(
          new ChannelUID(getThing().getUID(), OcppBindingConstants.LOCK.getAsString()), OnOffType.OFF);
    });
  }

  private void writeHardwareMaxCurrent(int amps) {
    if (ocppSender == null || chargerSerialNumber == null) {
      return;
    }
    if (hardwareMaxCurrentKey == null || hardwareMaxCurrentKey.trim().isEmpty()) {
      logger.warn("hardwareMaxCurrentKey not configured on {}; cannot write hardware max current",
          getThing().getUID());
      return;
    }
    ChargerReference reference = new ChargerReference(chargerSerialNumber);
    ocppSender.send(reference, new ChangeConfigurationRequest(hardwareMaxCurrentKey, Integer.toString(amps)))
        .whenComplete((confirmation, ex) -> {
          if (ex != null) {
            logger.warn("ChangeConfiguration[{}={}] for {} failed: {}", hardwareMaxCurrentKey, amps,
                getThing().getUID(), ex.getMessage());
          } else {
            logger.debug("ChangeConfiguration[{}={}] for {}: {}", hardwareMaxCurrentKey, amps,
                getThing().getUID(), confirmation);
          }
        });
  }

  private void readHardwareMaxCurrent() {
    if (ocppSender == null || chargerSerialNumber == null
        || hardwareMaxCurrentKey == null || hardwareMaxCurrentKey.trim().isEmpty()) {
      return;
    }
    ChargerReference reference = new ChargerReference(chargerSerialNumber);
    GetConfigurationRequest request = new GetConfigurationRequest();
    request.setKey(new String[]{ hardwareMaxCurrentKey });
    ocppSender.<GetConfigurationConfirmation>send(reference, request).whenComplete((confirmation, ex) -> {
      if (ex != null || confirmation == null || confirmation.getConfigurationKey() == null) {
        if (ex != null) {
          logger.warn("GetConfiguration[{}] for {} failed: {}", hardwareMaxCurrentKey, getThing().getUID(),
              ex.getMessage());
        }
        return;
      }
      for (KeyValueType kv : confirmation.getConfigurationKey()) {
        if (hardwareMaxCurrentKey.equals(kv.getKey()) && kv.getValue() != null) {
          try {
            double amps = Double.parseDouble(kv.getValue().trim());
            getCallback().stateUpdated(
                new ChannelUID(getThing().getUID(), OcppBindingConstants.HARDWARE_MAX_CURRENT.getAsString()),
                new QuantityType<>(amps, Units.AMPERE));
          } catch (NumberFormatException e) {
            logger.debug("Could not parse {}={} as a current value", hardwareMaxCurrentKey, kv.getValue());
          }
        }
      }
    });
  }

  @Override
  public MeterValuesConfirmation handleMeterValues(MeterValuesRequest request) {
    lastMeterValuesMs = System.currentTimeMillis();
    ThingHandlerCallback callback = getCallback();

    // push transaction id
    Integer transactionId = request.getTransactionId();
    // transactionId is null outside of an active charge transaction
    if (transactionId != null) {
      setCurrentTransactionId(transactionId);
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
        } catch (RuntimeException e) {
          // One bad sample must never escalate to a CallError for the whole MeterValues request —
          // OCPP expects the CSMS to acknowledge it regardless of how presentable the data is.
          logger.debug("Could not process measurement {}", sample, e);
        }
      }
    }

    return new MeterValuesConfirmation();
  }

  @Override
  public StatusNotificationConfirmation handleStatusNotification(StatusNotificationRequest request) {
    ChargePointStatus status = request.getStatus();
    lastStatus = status;
    lastStatusNotificationMs = System.currentTimeMillis();
    if (status != ChargePointStatus.Unavailable) {
      operativeRestorePending = false; // the connector left Unavailable — restore succeeded
    }

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
    pollMeterValuesIfStale();
    reconfirmStatusIfStale();
    reAssertOperativeIfPending();
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

  /**
   * Re-pull this connector's status when it has been frozen in a "busy" state too long. A
   * StatusNotification dropped during a session close/reconnect leaves the connector stuck (e.g. a
   * SuspendedEV the charger has since left), and a charge point only re-reports on a status CHANGE,
   * so the stale value would persist indefinitely. When a cable is believed connected but neither a
   * StatusNotification nor a MeterValues sample has arrived within {@link #STATUS_RECONFIRM_AFTER_MS},
   * ask the charger to re-send status for THIS connector — connectorId is set, since a multi-connector
   * charger (e.g. Phoenix CHARX) does not answer a connectorId-less StatusNotification trigger
   * per connector. A genuine status is simply re-confirmed; a stale one self-corrects.
   */
  private void reconfirmStatusIfStale() {
    long now = System.currentTimeMillis();
    if (!shouldReconfirmStatus(lastStatus, now, lastStatusNotificationMs, lastMeterValuesMs,
        lastStatusReconfirmMs, STATUS_RECONFIRM_AFTER_MS)) {
      return;
    }
    Integer connector = resolveConnectorId();
    if (ocppSender == null || chargerSerialNumber == null || connector == null) {
      return;
    }
    lastStatusReconfirmMs = now;
    ChargerReference reference = new ChargerReference(chargerSerialNumber);
    TriggerMessageRequest trigger = new TriggerMessageRequest(TriggerMessageRequestType.StatusNotification);
    trigger.setConnectorId(connector);
    ocppSender.send(reference, trigger).whenComplete((confirmation, ex) -> {
      if (ex != null) {
        logger.debug("Status re-confirm TriggerMessage(StatusNotification) for {} failed: {}",
            getThing().getUID(), ex.getMessage());
      } else {
        logger.debug("Status re-confirm TriggerMessage(StatusNotification) for {}: {}",
            getThing().getUID(), confirmation);
      }
    });
  }

  /**
   * Pure decision for {@link #reconfirmStatusIfStale()} — package-private for unit testing. Re-confirm
   * only when the connector believes a cable is connected (a "busy" status that can go stale) and it
   * has been silent — no StatusNotification, no MeterValues, and no prior re-confirm — for at least
   * {@code intervalMs}. An idle/terminal status (Available, Unavailable, Faulted) or any recent
   * activity suppresses it.
   */
  static boolean shouldReconfirmStatus(ChargePointStatus status, long now, long lastStatusMs,
      long lastMeterValuesMs, long lastReconfirmMs, long intervalMs) {
    if (status == null || !isCableConnected(status)) {
      return false;
    }
    return now - lastStatusMs >= intervalMs
        && now - lastMeterValuesMs >= intervalMs
        && now - lastReconfirmMs >= intervalMs;
  }

  /**
   * Pull a fresh MeterValues sample with TriggerMessage(MeterValues) when a cable is connected but no
   * samples have arrived within {@code meterPollIntervalMs}. Chargers push MeterValues only during an
   * active transaction (per MeterValueSampleInterval); a charger delivering power without an OCPP
   * transaction — or one whose transaction predates the server connection — never pushes, so the
   * power/energy channels would stay at zero. This is the server-side fallback. Disabled (interval 0)
   * for chargers without an internal meter (metered externally, e.g. CHARX over Modbus).
   */
  private void pollMeterValuesIfStale() {
    if (meterPollIntervalMs <= 0) {
      return;
    }
    ChargePointStatus status = lastStatus;
    if (status == null || !isCableConnected(status)) {
      return;
    }
    long now = System.currentTimeMillis();
    if (now - lastMeterValuesMs < meterPollIntervalMs || now - lastMeterPollMs < meterPollIntervalMs) {
      return;
    }
    Integer connector = resolveConnectorId();
    if (ocppSender == null || chargerSerialNumber == null || connector == null) {
      return;
    }
    lastMeterPollMs = now;
    ChargerReference reference = new ChargerReference(chargerSerialNumber);
    TriggerMessageRequest trigger = new TriggerMessageRequest(TriggerMessageRequestType.MeterValues);
    trigger.setConnectorId(connector);
    ocppSender.send(reference, trigger).whenComplete((confirmation, ex) -> {
      if (ex != null) {
        logger.debug("TriggerMessage(MeterValues) poll for {} failed: {}", getThing().getUID(), ex.getMessage());
      } else {
        logger.debug("TriggerMessage(MeterValues) poll for {}: {}", getThing().getUID(), confirmation);
      }
    });
  }

  private void cycleAvailability(ChargerReference reference, Integer connector) {
    ocppSender.send(reference, new ChangeAvailabilityRequest(connector, AvailabilityType.Inoperative))
        .whenComplete((confirmation, ex) -> {
          if (ex != null) {
            logger.warn("ChangeAvailability(Inoperative) for {} failed: {}", getThing().getUID(), ex.getMessage());
            return;
          }
          operativeRestorePending = true; // we now owe this connector an Operative; the watchdog guarantees it
          scheduler.schedule(this::restoreOperative, AVAILABILITY_RESTORE_DELAY_MS, TimeUnit.MILLISECONDS);
        });
  }

  /**
   * Send ChangeAvailability(Operative) to undo a binding-initiated Inoperative, clearing the pending
   * marker only once the CALL is actually accepted. A send lost on a dropped session leaves the marker
   * set so {@link #reAssertOperativeIfPending()} retries it — a connector can no longer strand Unavailable.
   */
  private void restoreOperative() {
    Integer connector = resolveConnectorId();
    if (ocppSender == null || chargerSerialNumber == null || connector == null) {
      return;
    }
    lastOperativeRetryMs = System.currentTimeMillis();
    ChargerReference reference = new ChargerReference(chargerSerialNumber);
    ocppSender.send(reference, new ChangeAvailabilityRequest(connector, AvailabilityType.Operative))
        .whenComplete((confirmation, ex) -> {
          if (ex != null) {
            logger.warn("ChangeAvailability(Operative) restore for {} failed: {} — will retry",
                getThing().getUID(), ex.getMessage());
          } else {
            logger.info("ChangeAvailability(Operative) restore for {}: {}", getThing().getUID(), confirmation);
            operativeRestorePending = false;
          }
        });
  }

  /** Re-assert a pending Operative restore (rate-limited) until the charger accepts it. */
  private void reAssertOperativeIfPending() {
    if (!operativeRestorePending
        || System.currentTimeMillis() - lastOperativeRetryMs < OPERATIVE_RETRY_INTERVAL_MS) {
      return;
    }
    restoreOperative();
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

    int txId = generateId();
    setCurrentTransactionId(txId);

    ThingHandlerCallback callback = getCallback();
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "idTag"), new StringType(tag));
    callback.stateUpdated(new ChannelUID(getThing().getUID(), OcppBindingConstants.CHARGING.getAsString()), OnOffType.ON);
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "timestampStart"), new DateTimeType(request.getTimestamp()));
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "meterStart"), new QuantityType<>(request.getMeterStart(), Units.WATT_HOUR));

    IdTagInfo tagInfo = new IdTagInfo(AuthorizationStatus.Accepted);
    return new StartTransactionConfirmation(tagInfo, txId);
  }

  @Override
  public StopTransactionConfirmation handleStopTransaction(StopTransactionRequest request) {
    String tag = request.getIdTag();

    Integer txId = request.getTransactionId();
    // Only act on a stop for this connector's own running transaction; with one charge point exposing
    // several connectors a StopTransaction can be dispatched here for another connector's id.
    if (currentTransactionId == null || !currentTransactionId.equals(txId)) {
      return new StopTransactionConfirmation();
    }

    setCurrentTransactionId(null);
    ThingHandlerCallback callback = getCallback();
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "idTag"), new StringType(tag));
    callback.stateUpdated(new ChannelUID(getThing().getUID(), OcppBindingConstants.CHARGING.getAsString()), OnOffType.OFF);
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "timestampStop"), new DateTimeType(request.getTimestamp()));
    callback.stateUpdated(new ChannelUID(getThing().getUID(), "meterStop"), new QuantityType<>(request.getMeterStop(), Units.WATT_HOUR));

    return new StopTransactionConfirmation();
  }

  /**
   * Every UnitOfMeasure value the OCPP 1.6 schema allows on a SampledValue, mapped straight to a
   * unit. The official enum contains several spellings JSR-385 parsers reject — {@code Celcius}
   * (the original misspelling is a legal wire value alongside {@code Celsius}), {@code Percent},
   * {@code Hertz}, and the reactive/apparent family ({@code var}, {@code kvar}, {@code varh},
   * {@code kvarh}, {@code VA}, {@code kVA}) — so relying on parsing alone turns a legal unit into
   * a failed request.
   */
  private static final java.util.Map<String, javax.measure.Unit<?>> OCPP_UNITS = java.util.Map.ofEntries(
      java.util.Map.entry("Wh", Units.WATT_HOUR),
      java.util.Map.entry("kWh", Units.KILOWATT_HOUR),
      java.util.Map.entry("varh", Units.VAR_HOUR),
      java.util.Map.entry("kvarh", Units.KILOVAR_HOUR),
      java.util.Map.entry("W", Units.WATT),
      java.util.Map.entry("kW", Units.WATT.multiply(1000)),
      java.util.Map.entry("VA", Units.VOLT_AMPERE),
      java.util.Map.entry("kVA", Units.KILOVOLT_AMPERE),
      java.util.Map.entry("var", Units.VAR),
      java.util.Map.entry("kvar", Units.KILOVAR),
      java.util.Map.entry("A", Units.AMPERE),
      java.util.Map.entry("V", Units.VOLT),
      java.util.Map.entry("K", Units.KELVIN),
      java.util.Map.entry("Celcius", org.openhab.core.library.unit.SIUnits.CELSIUS),
      java.util.Map.entry("Celsius", org.openhab.core.library.unit.SIUnits.CELSIUS),
      java.util.Map.entry("Fahrenheit", org.openhab.core.library.unit.ImperialUnits.FAHRENHEIT),
      java.util.Map.entry("Percent", Units.PERCENT),
      java.util.Map.entry("Hertz", Units.HERTZ)
  );

  static State parse(Double measurement, ChannelUID uid, SampledValue sample) {
    String unit = sample.getUnit();
    if (unit == null || unit.isEmpty()) {
      // default assumed from specs, when unit is not specified it fall backs to "Wh"
      return new QuantityType<>(measurement, Units.WATT_HOUR);
    }
    javax.measure.Unit<?> mapped = OCPP_UNITS.get(unit);
    if (mapped != null) {
      return new QuantityType<>(measurement, mapped);
    }
    try {
      // Vendor extension beyond the official enum — best-effort parse.
      Quantity<?> quantity = Quantities.getQuantity("1 " + unit);
      return new QuantityType<>(measurement, quantity.getUnit());
    } catch (RuntimeException e) {
      // A unit we can't express must never fail the request — deliver the bare number instead.
      return new DecimalType(measurement);
    }
  }

  private int generateId() {
    int transaction = transactionId.getAndIncrement();
    if (transaction == Integer.MAX_VALUE) {
      transactionId.set(0);
    }
    return transaction;
  }

}
