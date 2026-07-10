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

import eu.chargetime.ocpp.model.core.DataTransferConfirmation;
import eu.chargetime.ocpp.model.core.DataTransferRequest;
import eu.chargetime.ocpp.model.core.DataTransferStatus;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DataTransferAdapterTest {

  @Test
  void answersUnknownVendorIdInsteadOfLeavingTheRequestUnanswered() {
    // OCPP 1.6: a receiver with no implementation for the vendorId SHALL return status
    // UnknownVendorId (with no data element) — an unanswered request becomes a CallError
    // NotSupported, which is a protocol violation for DataTransfer.
    DataTransferAdapter adapter = new DataTransferAdapter();
    DataTransferRequest request = new DataTransferRequest("com.example.vendor");
    request.setMessageId("SomeMessage");

    DataTransferConfirmation confirmation = adapter.handleDataTransferRequest(UUID.randomUUID(), request);

    assertThat(confirmation).isNotNull();
    assertThat(confirmation.getStatus()).isEqualTo(DataTransferStatus.UnknownVendorId);
    assertThat(confirmation.getData()).isNull();
    assertThat(confirmation.validate()).isTrue();
  }
}
