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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import eu.chargetime.ocpp.model.core.SampledValue;
import org.junit.jupiter.api.Test;
import org.openhab.core.library.types.DecimalType;
import org.openhab.core.library.types.QuantityType;
import org.openhab.core.library.unit.ImperialUnits;
import org.openhab.core.library.unit.SIUnits;
import org.openhab.core.library.unit.Units;
import org.openhab.core.thing.ChannelUID;
import org.openhab.core.types.State;

/**
 * {@link ConnectorThingHandler#parse} must be able to express EVERY unit the OCPP 1.6 schema
 * allows on a SampledValue — including the officially misspelled {@code Celcius} and the
 * reactive/apparent family JSR-385 parsers reject — and must never throw: a unit failure used to
 * escape as a RuntimeException and turn the whole MeterValues request into a CallError.
 *
 * <p>Samples are mocked rather than built with {@code setUnit(...)} deliberately: the embedded
 * library's setter validates against its own (pre-errata, incomplete) unit list and throws for
 * legal wire values like {@code Celcius}/{@code Hertz} — but incoming requests are deserialized
 * by gson straight into fields, bypassing setters, so on the wire these values DO reach this
 * code. The mock reproduces the deserialized shape.
 */
class ConnectorThingHandlerUnitParseTest {

  private static final ChannelUID CHANNEL = new ChannelUID("co7io-ocpp:connector:bridge:charger:connector:soc");

  private static State parse(String unit) {
    SampledValue sample = mock(SampledValue.class);
    when(sample.getUnit()).thenReturn(unit);
    return ConnectorThingHandler.parse(21.5, CHANNEL, sample);
  }

  @Test
  void everyOfficialOcppUnitProducesAQuantity() {
    String[] officialEnum = {"Wh", "kWh", "varh", "kvarh", "W", "kW", "VA", "kVA", "var", "kvar",
        "A", "V", "K", "Celcius", "Celsius", "Fahrenheit", "Percent", "Hertz"};
    for (String unit : officialEnum) {
      State state = parse(unit);
      assertThat(state).as("unit '%s' must map to a QuantityType", unit).isInstanceOf(QuantityType.class);
    }
  }

  @Test
  void officiallyMisspelledCelciusMapsToCelsius() {
    assertThat(parse("Celcius")).isEqualTo(new QuantityType<>(21.5, SIUnits.CELSIUS));
    assertThat(parse("Celsius")).isEqualTo(new QuantityType<>(21.5, SIUnits.CELSIUS));
    assertThat(parse("Fahrenheit")).isEqualTo(new QuantityType<>(21.5, ImperialUnits.FAHRENHEIT));
  }

  @Test
  void percentAndReactiveUnitsMapDirectly() {
    assertThat(parse("Percent")).isEqualTo(new QuantityType<>(21.5, Units.PERCENT));
    assertThat(parse("var")).isEqualTo(new QuantityType<>(21.5, Units.VAR));
    assertThat(parse("kVA")).isEqualTo(new QuantityType<>(21.5, Units.KILOVOLT_AMPERE));
  }

  @Test
  void missingUnitDefaultsToWattHourPerSpec() {
    assertThat(parse(null)).isEqualTo(new QuantityType<>(21.5, Units.WATT_HOUR));
    assertThat(parse("")).isEqualTo(new QuantityType<>(21.5, Units.WATT_HOUR));
  }

  @Test
  void unparseableVendorUnitFallsBackToBareNumberInsteadOfThrowing() {
    assertThat(parse("FooBarUnit")).isEqualTo(new DecimalType(21.5));
  }

  @Test
  void parseableVendorExtensionStillGoesThroughTheParser() {
    // Not in the official enum, but a sane vendor extension JSR-385 understands.
    State state = parse("Hz");
    assertThat(state).isInstanceOf(QuantityType.class);
  }
}
