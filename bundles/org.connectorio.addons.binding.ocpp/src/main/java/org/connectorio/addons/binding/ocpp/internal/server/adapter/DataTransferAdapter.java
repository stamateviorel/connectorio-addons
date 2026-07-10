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

import eu.chargetime.ocpp.model.core.DataTransferConfirmation;
import eu.chargetime.ocpp.model.core.DataTransferRequest;
import eu.chargetime.ocpp.model.core.DataTransferStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Catch-all DataTransfer responder. OCPP 1.6 requires the receiver of a DataTransfer.req it has
 * no implementation for to answer with a DataTransferConfirmation carrying status
 * {@code UnknownVendorId} (and no data element) — NOT a protocol-level error. Without this
 * adapter no handler in the chain produces a confirmation, and the library then answers with a
 * CallError {@code NotSupported}, which some charge points treat as a transport fault and retry.
 *
 * <p>Registered LAST in the handler chain, so any future vendor-specific DataTransfer handler
 * placed before it wins (the chain takes the first valid confirmation).
 */
public class DataTransferAdapter extends CoreEventHandlerAdapter {

  private final Logger logger = LoggerFactory.getLogger(DataTransferAdapter.class);

  @Override
  public DataTransferConfirmation handleDataTransferRequest(UUID sessionIndex, DataTransferRequest request) {
    logger.debug("DataTransfer from session {} for unhandled vendorId '{}' (messageId '{}') — answering UnknownVendorId",
        sessionIndex, request.getVendorId(), request.getMessageId());
    return new DataTransferConfirmation(DataTransferStatus.UnknownVendorId);
  }
}
