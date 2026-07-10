package org.connectorio.addons.binding.ocpp.internal.server.adapter;

import eu.chargetime.ocpp.feature.profile.ServerCoreEventHandler;
import eu.chargetime.ocpp.model.Confirmation;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;

public class CoreEventHandlerAdapter implements ServerCoreEventHandler {

  /** Chargers whose boot-time config burst has fully succeeded — never retried again. */
  private final Set<String> configuredChargers = ConcurrentHashMap.newKeySet();
  /** Chargers with a boot-time config burst currently in flight — guards re-entrant firing. */
  private final Set<String> configInProgress = ConcurrentHashMap.newKeySet();
  /** Per-charger override of {@link #CONFIG_SETTLE_SECONDS}, keyed by serial. */
  private final Map<String, Long> configSettleOverrides = new ConcurrentHashMap<>();

  /**
   * Default delay before the boot-time configuration burst is sent, for chargers without a
   * {@link #setConfigSettleSeconds} override. A charger that just (re)booted announces itself
   * with a BootNotification before its OCPP stack is ready to answer ChangeConfiguration; firing
   * the burst immediately gets the first CALL timed out, which closes the freshly established
   * session and delays charging. Sending it via {@code OcppSender.sendAfter} a few seconds later
   * lets the charger settle. These config values are persisted charger-side anyway, so a missed
   * attempt is harmless — {@link #firstBootForConfig} only latches once the burst actually
   * succeeds, so a charger that needs longer than this default gets a clean retry on its next
   * genuine reboot.
   */
  protected static final long CONFIG_SETTLE_SECONDS = 10;

  /**
   * Per-charger override for how long to wait after BootNotification before firing the
   * boot-time config burst. Some chargers' internal OCPP/config agents take longer than
   * {@link #CONFIG_SETTLE_SECONDS} to come up after a controller restart (observed on Phoenix
   * Contact CHARX: the default 10 s was not enough, so the first queued ChangeConfiguration
   * timed out and closed the session before its own agent was ready to answer).
   */
  public void setConfigSettleSeconds(String serial, long seconds) {
    if (serial != null && seconds > 0) {
      configSettleOverrides.put(serial, seconds);
    }
  }

  protected long settleSecondsFor(ChargerReference reference) {
    if (reference == null || reference.getSerial() == null) {
      return CONFIG_SETTLE_SECONDS;
    }
    return configSettleOverrides.getOrDefault(reference.getSerial(), CONFIG_SETTLE_SECONDS);
  }

  /**
   * Returns {@code true} if this charger's boot-time config burst should be (re)attempted: it has
   * neither already succeeded nor is one currently in flight. Unlike a plain one-shot gate, a
   * burst that fails (e.g. a ChangeConfiguration CALL timing out and closing the session) does
   * NOT permanently block retries — {@link #markConfigFailed} clears the in-progress marker so
   * the next genuine BootNotification (the only event this is invoked from) gets a clean attempt.
   * {@link #markConfigSucceeded} is what actually latches a charger as done. Private: the only
   * sanctioned entry point is {@link #runBootConfigBurst} — see its javadoc for why.
   */
  private boolean firstBootForConfig(ChargerReference reference) {
    if (reference == null || reference.getSerial() == null) {
      return false;
    }
    String serial = reference.getSerial();
    if (configuredChargers.contains(serial)) {
      return false;
    }
    return configInProgress.add(serial);
  }

  private void markConfigSucceeded(ChargerReference reference) {
    if (reference == null || reference.getSerial() == null) {
      return;
    }
    configuredChargers.add(reference.getSerial());
    configInProgress.remove(reference.getSerial());
  }

  private void markConfigFailed(ChargerReference reference) {
    if (reference != null && reference.getSerial() != null) {
      configInProgress.remove(reference.getSerial());
    }
  }

  /**
   * Waits on every stage of a boot-time config burst and marks the charger succeeded only if
   * every key was accepted; any single failure marks it failed instead, so the whole burst
   * (harmless to repeat — values persist charger-side) is retried on the next genuine reboot.
   */
  private void completeConfigAttempt(ChargerReference reference, List<? extends CompletionStage<Confirmation>> stages) {
    CompletableFuture<?>[] futures = stages.stream()
        .map(CompletionStage::toCompletableFuture)
        .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).whenComplete((done, ex) -> {
      if (ex == null) {
        markConfigSucceeded(reference);
      } else {
        markConfigFailed(reference);
      }
    });
  }

  /**
   * The ONLY way a subclass may participate in the once-per-charger boot-time config burst: check
   * the gate, invoke {@code keyPusher} to fire the actual ChangeConfiguration calls, and mark
   * success/failure from their aggregate outcome — as a single, indivisible operation.
   *
   * <p>Before this existed, three adapters each hand-rolled "check {@code firstBootForConfig},
   * build a stage list, call {@code completeConfigAttempt}" independently. That let a subclass
   * gate without ever completing (permanently stuck "in progress" — worse than the original bug,
   * since it would never retry at all), complete against a different/incomplete stage list than
   * what it actually sent (marking success on a burst that partially failed), or skip the gate
   * entirely. {@code firstBootForConfig}/{@code markConfigSucceeded}/{@code markConfigFailed}/
   * {@code completeConfigAttempt} are private specifically so a new adapter cannot reintroduce any
   * of those mistakes — this method is the only door in.
   *
   * <p>{@code keyPusher} is only invoked when the gate admits the attempt, so it may safely use
   * {@code reference} without a further null check.
   */
  protected final void runBootConfigBurst(ChargerReference reference,
      Supplier<List<CompletionStage<Confirmation>>> keyPusher) {
    if (reference == null || !firstBootForConfig(reference)) {
      return;
    }
    completeConfigAttempt(reference, keyPusher.get());
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
