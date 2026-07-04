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
package org.connectorio.addons.binding.ocpp.internal.handler;

import static org.assertj.core.api.Assertions.assertThat;

import eu.chargetime.ocpp.model.core.ChargePointStatus;
import org.junit.jupiter.api.Test;

class ConnectorThingHandlerStatusReconfirmTest {

  private static final long INTERVAL = 300_000L;
  private static final long NOW = 10_000_000L;

  @Test
  void reconfirmsAStaleBusyStatusWithNoRecentActivity() {
    // Charging, but no StatusNotification / MeterValues / prior re-confirm within the window.
    assertThat(ConnectorThingHandler.shouldReconfirmStatus(
        ChargePointStatus.Charging, NOW, NOW - INTERVAL, NOW - INTERVAL, NOW - INTERVAL, INTERVAL))
        .isTrue();
    assertThat(ConnectorThingHandler.shouldReconfirmStatus(
        ChargePointStatus.SuspendedEV, NOW, 0, 0, 0, INTERVAL))
        .isTrue();
  }

  @Test
  void neverReconfirmsAnIdleOrTerminalStatus() {
    for (ChargePointStatus idle : new ChargePointStatus[] {
        ChargePointStatus.Available, ChargePointStatus.Unavailable, ChargePointStatus.Faulted}) {
      assertThat(ConnectorThingHandler.shouldReconfirmStatus(idle, NOW, 0, 0, 0, INTERVAL))
          .as("idle status %s must not be re-confirmed", idle)
          .isFalse();
    }
  }

  @Test
  void nullStatusIsNeverReconfirmed() {
    assertThat(ConnectorThingHandler.shouldReconfirmStatus(null, NOW, 0, 0, 0, INTERVAL)).isFalse();
  }

  @Test
  void recentActivitySuppressesReconfirm() {
    // A fresh StatusNotification just arrived → not stale yet.
    assertThat(ConnectorThingHandler.shouldReconfirmStatus(
        ChargePointStatus.Charging, NOW, NOW - 1, NOW - INTERVAL, NOW - INTERVAL, INTERVAL))
        .isFalse();
    // A fresh MeterValues sample (metered charger actively charging) → we know it is alive.
    assertThat(ConnectorThingHandler.shouldReconfirmStatus(
        ChargePointStatus.Charging, NOW, NOW - INTERVAL, NOW - 1, NOW - INTERVAL, INTERVAL))
        .isFalse();
    // Already re-confirmed within the window → do not spam.
    assertThat(ConnectorThingHandler.shouldReconfirmStatus(
        ChargePointStatus.Charging, NOW, NOW - INTERVAL, NOW - INTERVAL, NOW - 1, INTERVAL))
        .isFalse();
  }

  @Test
  void allBusyStatesAreEligible() {
    for (ChargePointStatus busy : new ChargePointStatus[] {
        ChargePointStatus.Preparing, ChargePointStatus.Charging, ChargePointStatus.SuspendedEV,
        ChargePointStatus.SuspendedEVSE, ChargePointStatus.Finishing}) {
      assertThat(ConnectorThingHandler.shouldReconfirmStatus(busy, NOW, 0, 0, 0, INTERVAL))
          .as("busy status %s should be eligible when stale", busy)
          .isTrue();
    }
  }
}
