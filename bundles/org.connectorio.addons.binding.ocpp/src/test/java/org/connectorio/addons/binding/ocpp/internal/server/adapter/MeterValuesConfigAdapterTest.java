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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

import eu.chargetime.ocpp.model.Confirmation;
import eu.chargetime.ocpp.model.Request;
import eu.chargetime.ocpp.model.core.BootNotificationRequest;
import eu.chargetime.ocpp.model.core.ChangeConfigurationRequest;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.connectorio.addons.binding.ocpp.internal.OcppSender;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;
import org.connectorio.addons.binding.ocpp.internal.server.OcppChargerSessionRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MeterValuesConfigAdapterTest {

  @Test
  void aMeterlessChargerOnlyGetsClockAlignedDataDisabled() {
    ChargerReference charx = new ChargerReference("charx");
    UUID session = UUID.randomUUID();
    OcppChargerSessionRegistry registry = mock(OcppChargerSessionRegistry.class);
    when(registry.getCharger(session)).thenReturn(charx);
    OcppSender sender = mock(OcppSender.class);
    CompletionStage<Confirmation> pending = new CompletableFuture<>();
    when(sender.sendAfter(any(ChargerReference.class), any(Request.class), anyLong())).thenReturn(pending);

    MeterValuesConfigAdapter adapter = new MeterValuesConfigAdapter(registry, sender, 30,
        "Energy.Active.Import.Register,Voltage", 30, Set.of("charx"));

    adapter.handleBootNotificationRequest(session, new BootNotificationRequest());

    // Exactly one corrective ChangeConfiguration — disabling the periodic clock-aligned emission
    // that would otherwise run forever from a previously-configured interval — nothing else.
    verify(sender, times(1)).sendAfter(any(ChargerReference.class), any(Request.class), anyLong());
    ArgumentCaptor<Request> captor = ArgumentCaptor.forClass(Request.class);
    verify(sender).sendAfter(eq(charx), captor.capture(), anyLong());
    ChangeConfigurationRequest request = (ChangeConfigurationRequest) captor.getValue();
    assertThat(request.getKey()).isEqualTo("ClockAlignedDataInterval");
    assertThat(request.getValue()).isEqualTo("0");
  }

  @Test
  void stillConfiguresAChargerNotInTheMeterlessSet() {
    ChargerReference wallbox = new ChargerReference("wallbox");
    UUID session = UUID.randomUUID();
    OcppChargerSessionRegistry registry = mock(OcppChargerSessionRegistry.class);
    when(registry.getCharger(session)).thenReturn(wallbox);
    OcppSender sender = mock(OcppSender.class);
    CompletionStage<Confirmation> pending = new CompletableFuture<>();
    when(sender.sendAfter(any(ChargerReference.class), any(Request.class), anyLong())).thenReturn(pending);

    MeterValuesConfigAdapter adapter = new MeterValuesConfigAdapter(registry, sender, 30,
        "Energy.Active.Import.Register,Voltage", 30, Set.of("charx"));

    adapter.handleBootNotificationRequest(session, new BootNotificationRequest());

    verify(sender, times(4)).sendAfter(any(ChargerReference.class), any(Request.class), anyLong());
  }

  @Test
  void stripsTemperatureFirst() {
    assertThat(MeterValuesConfigAdapter.stripFirstFragile(
        "Energy.Active.Import.Register,Power.Active.Import,Current.Import,Voltage,Temperature"))
        .isEqualTo("Energy.Active.Import.Register,Power.Active.Import,Current.Import,Voltage");
  }

  @Test
  void stripsPowerOfferedWhenTemperatureAbsent() {
    assertThat(MeterValuesConfigAdapter.stripFirstFragile(
        "Energy.Active.Import.Register,Power.Offered,Current.Import,Voltage"))
        .isEqualTo("Energy.Active.Import.Register,Current.Import,Voltage");
  }

  @Test
  void stripsTemperatureBeforePowerOffered() {
    assertThat(MeterValuesConfigAdapter.stripFirstFragile(
        "Power.Offered,Temperature,Current.Import"))
        .isEqualTo("Power.Offered,Current.Import");
  }

  @Test
  void leavesUntouchedWhenNoFragileMeasurand() {
    assertThat(MeterValuesConfigAdapter.stripFirstFragile("Energy.Active.Import.Register,Voltage"))
        .isEqualTo("Energy.Active.Import.Register,Voltage");
  }

  @Test
  void handlesWhitespaceAroundCommas() {
    assertThat(MeterValuesConfigAdapter.stripFirstFragile("Voltage, Temperature, Current.Import"))
        .isEqualTo("Voltage,Current.Import");
  }

  @Test
  void handlesNullAndEmpty() {
    assertThat(MeterValuesConfigAdapter.stripFirstFragile(null)).isNull();
    assertThat(MeterValuesConfigAdapter.stripFirstFragile("")).isEmpty();
  }
}
