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
package org.connectorio.addons.binding.ocpp.internal;

import eu.chargetime.ocpp.model.Confirmation;
import eu.chargetime.ocpp.model.Request;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;

public interface OcppSender {

  <T extends Confirmation> CompletionStage<T> send(ChargerReference reference, Request request);

  /**
   * Send a CALL after a settle delay. Used for best-effort boot-time configuration so the burst is
   * not fired the instant a (re)booted charger announces itself — a charger that just rebooted is
   * often not yet ready to answer ChangeConfiguration, and an unanswered CALL times out and closes
   * the freshly established session (see the timeout handling in the server's session sender),
   * delaying charging. Deferring the burst lets the charger settle first. The default sends
   * immediately; the server overrides it to actually defer on its scheduler.
   */
  default <T extends Confirmation> CompletionStage<T> sendAfter(ChargerReference reference, Request request,
      long delaySeconds) {
    return send(reference, request);
  }

}
