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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

import eu.chargetime.ocpp.model.Confirmation;
import eu.chargetime.ocpp.model.Request;
import eu.chargetime.ocpp.model.core.BootNotificationRequest;
import eu.chargetime.ocpp.model.core.ChangeConfigurationConfirmation;
import eu.chargetime.ocpp.model.core.ConfigurationStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.connectorio.addons.binding.ocpp.internal.OcppSender;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;
import org.connectorio.addons.binding.ocpp.internal.server.OcppChargerSessionRegistry;
import org.junit.jupiter.api.Test;

class RemoteAuthorizationConfigAdapterTest {

  @Test
  void aFailedAttemptRetriesOnTheNextBootNotification() {
    ChargerReference charx = new ChargerReference("charx");
    UUID session = UUID.randomUUID();
    OcppChargerSessionRegistry registry = mock(OcppChargerSessionRegistry.class);
    when(registry.getCharger(session)).thenReturn(charx);
    OcppSender sender = mock(OcppSender.class);
    List<CompletableFuture<Confirmation>> issued = new ArrayList<>();
    when(sender.sendAfter(any(ChargerReference.class), any(Request.class), anyLong())).thenAnswer(invocation -> {
      CompletableFuture<Confirmation> future = new CompletableFuture<>();
      issued.add(future);
      return future;
    });

    RemoteAuthorizationConfigAdapter adapter = new RemoteAuthorizationConfigAdapter(registry, sender);

    adapter.handleBootNotificationRequest(session, new BootNotificationRequest());
    assertThat(issued).hasSize(1);
    issued.get(0).completeExceptionally(new RuntimeException("timed out"));

    adapter.handleBootNotificationRequest(session, new BootNotificationRequest());
    assertThat(issued).hasSize(2);
  }

  @Test
  void aSuccessfulAttemptDoesNotRetry() {
    ChargerReference charx = new ChargerReference("charx");
    UUID session = UUID.randomUUID();
    OcppChargerSessionRegistry registry = mock(OcppChargerSessionRegistry.class);
    when(registry.getCharger(session)).thenReturn(charx);
    OcppSender sender = mock(OcppSender.class);
    List<CompletableFuture<Confirmation>> issued = new ArrayList<>();
    when(sender.sendAfter(any(ChargerReference.class), any(Request.class), anyLong())).thenAnswer(invocation -> {
      CompletableFuture<Confirmation> future = new CompletableFuture<>();
      issued.add(future);
      return future;
    });

    RemoteAuthorizationConfigAdapter adapter = new RemoteAuthorizationConfigAdapter(registry, sender);

    adapter.handleBootNotificationRequest(session, new BootNotificationRequest());
    issued.get(0).complete(new ChangeConfigurationConfirmation(ConfigurationStatus.Accepted));

    adapter.handleBootNotificationRequest(session, new BootNotificationRequest());
    assertThat(issued).hasSize(1);
  }

  @Test
  void perChargerSettleSecondsOverrideIsPassedToSendAfter() {
    ChargerReference charx = new ChargerReference("charx");
    UUID session = UUID.randomUUID();
    OcppChargerSessionRegistry registry = mock(OcppChargerSessionRegistry.class);
    when(registry.getCharger(session)).thenReturn(charx);
    OcppSender sender = mock(OcppSender.class);
    when(sender.sendAfter(any(ChargerReference.class), any(Request.class), anyLong()))
        .thenReturn(new CompletableFuture<>());

    RemoteAuthorizationConfigAdapter adapter = new RemoteAuthorizationConfigAdapter(registry, sender);
    adapter.setConfigSettleSeconds("charx", 45);

    adapter.handleBootNotificationRequest(session, new BootNotificationRequest());

    verify(sender).sendAfter(eq(charx), any(Request.class), eq(45L));
  }
}
