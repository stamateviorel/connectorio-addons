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
package org.connectorio.addons.binding.ocpp.internal.server;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The liveness watchdog's silence threshold must track each charger's negotiated heartbeat — an
 * idle charger's only periodic OCPP message IS its heartbeat, so a threshold below the interval
 * force-cycles perfectly healthy sessions at heartbeat cadence. Live incident 2026-07-09/10:
 * heartbeat raised to 300s against the fixed 180s floor cycled CHARX's idle session every ~5
 * minutes for 19 hours.
 */
class StaleThresholdTest {

  @Test
  void thresholdForA300sHeartbeatComfortablyExceedsTheInterval() {
    // 2 missed heartbeats + one sweep interval of margin: 2*300 + 60 = 660s
    assertThat(OcppServer.staleThresholdMs(300)).isEqualTo(660_000L);
  }

  @Test
  void shortHeartbeatsKeepTheFixedFloor() {
    // 2*10+60 = 80s < 180s floor — fast-heartbeat chargers keep the responsive default
    assertThat(OcppServer.staleThresholdMs(10)).isEqualTo(180_000L);
    assertThat(OcppServer.staleThresholdMs(60)).isEqualTo(180_000L);
  }

  @Test
  void unknownOrInvalidIntervalFallsBackToTheFloor() {
    assertThat(OcppServer.staleThresholdMs(null)).isEqualTo(180_000L);
    assertThat(OcppServer.staleThresholdMs(0)).isEqualTo(180_000L);
    assertThat(OcppServer.staleThresholdMs(-5)).isEqualTo(180_000L);
  }

  @Test
  void thresholdIsAlwaysStrictlyAboveTheHeartbeatInterval() {
    for (int interval : new int[] {1, 10, 60, 120, 180, 300, 600, 3600}) {
      assertThat(OcppServer.staleThresholdMs(interval))
          .as("a healthy charger heartbeating every %ss must never be reaped", interval)
          .isGreaterThan(interval * 1000L);
    }
  }
}
