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
package org.connectorio.addons.binding.ocpp.internal.config;

import org.connectorio.addons.binding.config.Configuration;

public class ConnectorConfig implements Configuration {
  public static final String DEFAULT_REMOTE_START_TAG = "openhab";
  public Integer connectorId;
  public String remoteStartTag = DEFAULT_REMOTE_START_TAG;
  public String hardwareMaxCurrentKey;

  /**
   * How often, in seconds, to pull a fresh MeterValues sample with TriggerMessage(MeterValues) while a
   * cable is connected but no samples arrive on their own. 0 disables the poll — set 0 for chargers
   * without an internal energy meter (e.g. Phoenix Contact CHARX, metered externally over Modbus).
   */
  public Integer meterValuesPollSeconds = 30;

  /**
   * Always send charge-limit SetChargingProfile as a TxDefaultProfile (no transactionId) rather than a
   * per-transaction TxProfile. Some charge points (e.g. Phoenix Contact CHARX) Reject a TxProfile
   * whenever there is no active transaction — which happens while the connector is in B/Finishing or
   * just after a charger reboot before the transaction re-opens — so a transient limit is lost. A
   * TxDefaultProfile is accepted regardless of transaction state and persists across transactions and
   * charger reboots, which is the correct behaviour for current control on a meter-less charger (no
   * per-transaction accounting is needed). Leave false for chargers that require a TxProfile to apply a
   * mid-session limit immediately (metered Wallboxes).
   */
  public boolean forceTxDefaultProfile = false;
}
