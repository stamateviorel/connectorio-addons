/*
 * Copyright (C) 2022-2022 ConnectorIO Sp. z o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.connectorio.addons.binding.ocpp.internal.server.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import eu.chargetime.ocpp.NotConnectedException;
import eu.chargetime.ocpp.model.Confirmation;
import eu.chargetime.ocpp.model.Request;
import eu.chargetime.ocpp.model.core.BootNotificationRequest;
import eu.chargetime.ocpp.model.core.ChangeConfigurationConfirmation;
import eu.chargetime.ocpp.model.core.ConfigurationStatus;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import org.connectorio.addons.binding.ocpp.internal.OcppSender;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;
import org.connectorio.addons.binding.ocpp.internal.server.OcppChargerSessionRegistry;
import org.junit.jupiter.api.Test;

/**
 * Exercises the REAL interaction between the three boot-time config adapters
 * ({@link VendorConfigAdapter}, {@link MeterValuesConfigAdapter},
 * {@link RemoteAuthorizationConfigAdapter}) sharing one charger's serialized call queue — the
 * level at which the v32 CHARX bug actually lived. Every other test in this package mocks
 * {@code OcppSender} so each {@code sendAfter} call resolves independently; that's correct for
 * proving a single adapter's own retry/settle logic, but it can never reproduce a bug that only
 * appears when THREE adapters fire into the SAME single-CALL-in-flight session at once.
 *
 * <p>{@link FakeSerializingSender} is a faithful behavioral model of
 * {@code OcppServer.SessionSender}: one CALL in flight at a time, dispatched in
 * {@code sendAfter} delay order, and a CALL that isn't answered within its timeout window closes
 * the whole session — every other still-queued CALL fails as collateral, exactly like the real
 * {@code SessionSender}'s handling of ChargeTimeEU/Java-OCA-OCPP#121. It uses a virtual clock
 * ({@link FakeSerializingSender#deliverUpTo}) instead of real sockets or real waits, so this stays
 * fast and deterministic while still reproducing the actual constraint that caused the bug.
 */
class BootConfigBurstCollisionTest {

  private static Map<String, String> vendorKeys() {
    Map<String, String> keys = new LinkedHashMap<>();
    keys.put("WebSocketPingInterval", "30");
    keys.put("AvailabilityOnlyWhenTimeSynchronized", "false");
    return keys;
  }

  @Test
  void slowToWakeChargerCollidesOnDefaultSettle_thenSucceedsOnTheNextBootAfterASettleOverride() {
    ChargerReference charx = new ChargerReference("charx");
    UUID session = UUID.randomUUID();
    OcppChargerSessionRegistry registry = mock(OcppChargerSessionRegistry.class);
    when(registry.getCharger(session)).thenReturn(charx);

    FakeSerializingSender sender = new FakeSerializingSender();
    VendorConfigAdapter vendor = new VendorConfigAdapter(registry, sender, vendorKeys());
    MeterValuesConfigAdapter meterValues = new MeterValuesConfigAdapter(registry, sender, 30,
        "Energy.Active.Import.Register,Voltage", 30, Set.of("charx"));
    RemoteAuthorizationConfigAdapter remoteAuth = new RemoteAuthorizationConfigAdapter(registry, sender);
    List<CoreEventHandlerAdapter> adapters = List.of(vendor, meterValues, remoteAuth);

    // --- Boot 1: all three adapters at the 10s default settle; this charger's own config agent
    // isn't ready to answer ChangeConfiguration until 30s in (exactly the CHARX behavior that
    // caused the real bug — the first live reboot reproduced this precisely). ---
    adapters.forEach(a -> a.handleBootNotificationRequest(session, new BootNotificationRequest()));
    assertThat(sender.pendingCount())
        .as("vendor's 2 keys + meterValues' 1 meterless key + remoteAuth's 1 key, all fired at the same default settle")
        .isEqualTo(4);
    sender.deliverUpTo(30, 15);
    assertThat(sender.succeededCount()).as("nothing landed — the whole burst collided").isZero();
    assertThat(sender.failedCount()).isEqualTo(4);

    // --- Between boots: apply the fix, exactly as v32 was deployed between the two live reboots. ---
    adapters.forEach(a -> a.setConfigSettleSeconds("charx", 45));

    // --- Boot 2: a genuine second reboot. Same charger, same 30s-to-ready behavior — only the
    // settle delay changed. ---
    sender.startNewSession();
    adapters.forEach(a -> a.handleBootNotificationRequest(session, new BootNotificationRequest()));
    assertThat(sender.pendingCount())
        .as("the retry-safe gate must re-attempt the WHOLE burst on this genuine reboot — a "
            + "pre-v32 one-shot-on-attempt gate would show 0 here, since boot 1's failed attempt "
            + "would already have latched every adapter as \"configured\" forever")
        .isEqualTo(4);
    sender.deliverUpTo(30, 15);
    assertThat(sender.succeededCount()).as("30s charger-ready now fits inside the 45s+15s window").isEqualTo(4);
    assertThat(sender.failedCount()).isZero();

    // --- Boot 3: everything already landed — must not re-push on every future reboot. ---
    sender.startNewSession();
    adapters.forEach(a -> a.handleBootNotificationRequest(session, new BootNotificationRequest()));
    assertThat(sender.pendingCount()).as("already configured — no wasted re-push").isZero();
  }

  @Test
  void aChargerThatIsReadyInTimeNeverCollidesEvenWithoutAnOverride() {
    ChargerReference wallbox = new ChargerReference("wallbox");
    UUID session = UUID.randomUUID();
    OcppChargerSessionRegistry registry = mock(OcppChargerSessionRegistry.class);
    when(registry.getCharger(session)).thenReturn(wallbox);

    FakeSerializingSender sender = new FakeSerializingSender();
    VendorConfigAdapter vendor = new VendorConfigAdapter(registry, sender, vendorKeys());
    MeterValuesConfigAdapter meterValues = new MeterValuesConfigAdapter(registry, sender, 30,
        "Energy.Active.Import.Register,Voltage", 30, Set.of());
    RemoteAuthorizationConfigAdapter remoteAuth = new RemoteAuthorizationConfigAdapter(registry, sender);
    List<CoreEventHandlerAdapter> adapters = List.of(vendor, meterValues, remoteAuth);

    adapters.forEach(a -> a.handleBootNotificationRequest(session, new BootNotificationRequest()));
    // A well-behaved charger (default 10s settle, ready at 2s) never causes the multi-adapter
    // burst to collide, even with three independent adapters sharing the one session queue.
    sender.deliverUpTo(2, 15);

    assertThat(sender.succeededCount()).isEqualTo(7); // vendor's 2 + meterValues' 4 (not meterless) + remoteAuth's 1
    assertThat(sender.failedCount()).isZero();
  }

  /**
   * Faithful behavioral model of {@code OcppServer.SessionSender} — see the class javadoc above
   * for why a plain per-call mock cannot reproduce this bug. Not a general-purpose OCPP test
   * double: it only implements what the boot-config adapters actually use ({@code sendAfter}), and
   * deliberately does not model real sockets, real wall-clock time, or the chargetime/ocpp CALL-id
   * protocol — only the specific constraint that caused the v32 bug (one CALL in flight per
   * session; an unanswered CALL closes the session and fails every other queued CALL as
   * collateral).
   */
  private static final class FakeSerializingSender implements OcppSender {

    private final List<ScheduledCall> scheduled = new ArrayList<>();
    private boolean sessionClosed;
    private int succeeded;
    private int failed;

    @Override
    public <T extends Confirmation> CompletionStage<T> send(ChargerReference reference, Request request) {
      throw new UnsupportedOperationException("boot-config adapters only use sendAfter");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Confirmation> CompletionStage<T> sendAfter(ChargerReference reference, Request request,
        long delaySeconds) {
      CompletableFuture<Confirmation> result = new CompletableFuture<>();
      if (sessionClosed) {
        failed++;
        result.completeExceptionally(new NotConnectedException());
      } else {
        scheduled.add(new ScheduledCall(delaySeconds, result));
      }
      return (CompletionStage<T>) result;
    }

    /**
     * Simulates the charger becoming ready to answer ChangeConfiguration at
     * {@code chargerReadySeconds} after BootNotification, then dispatches every call still queued
     * in {@code sendAfter} delay order (the real SessionSender's single-worker dispatch order): the
     * first call whose {@code delaySeconds + callTimeoutSeconds} window is exceeded by
     * {@code chargerReadySeconds} times out and closes the session — every call after it (even ones
     * whose own window would have been fine) fails as collateral, exactly like a real
     * timeout-induced reconnect.
     */
    void deliverUpTo(long chargerReadySeconds, long callTimeoutSeconds) {
      scheduled.sort(Comparator.comparingLong(call -> call.delaySeconds));
      for (ScheduledCall call : scheduled) {
        if (sessionClosed) {
          failed++;
          call.result.completeExceptionally(new NotConnectedException());
          continue;
        }
        if (chargerReadySeconds > call.delaySeconds + callTimeoutSeconds) {
          failed++;
          call.result.completeExceptionally(new TimeoutException("simulated: charger not ready in time"));
          sessionClosed = true;
        } else {
          succeeded++;
          call.result.complete(new ChangeConfigurationConfirmation(ConfigurationStatus.Accepted));
        }
      }
      scheduled.clear();
    }

    /**
     * A genuine reboot opens a brand new WS session — nothing from the old one carries over.
     * {@code succeeded}/{@code failed} are per-session counts (reset here), not lifetime totals.
     */
    void startNewSession() {
      sessionClosed = false;
      scheduled.clear();
      succeeded = 0;
      failed = 0;
    }

    int pendingCount() {
      return scheduled.size();
    }

    int succeededCount() {
      return succeeded;
    }

    int failedCount() {
      return failed;
    }

    private static final class ScheduledCall {
      final long delaySeconds;
      final CompletableFuture<Confirmation> result;

      ScheduledCall(long delaySeconds, CompletableFuture<Confirmation> result) {
        this.delaySeconds = delaySeconds;
        this.result = result;
      }
    }
  }
}
