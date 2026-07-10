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

import eu.chargetime.ocpp.NotConnectedException;
import eu.chargetime.ocpp.feature.profile.ServerCoreEventHandler;
import eu.chargetime.ocpp.model.Request;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;
import org.connectorio.addons.binding.handler.GenericBridgeHandlerBase;
import org.connectorio.addons.binding.ocpp.internal.OcppAttendant;
import org.connectorio.addons.binding.ocpp.internal.OcppRequestListener;
import org.connectorio.addons.binding.ocpp.internal.OcppSender;
import org.connectorio.addons.binding.ocpp.internal.config.ServerConfig;
import org.connectorio.addons.binding.ocpp.internal.discovery.OcppChargerDiscoveryService;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;
import org.connectorio.addons.binding.ocpp.internal.server.CompositeRequestListener;
import org.connectorio.addons.binding.ocpp.internal.server.OcppServer;
import org.connectorio.addons.binding.ocpp.internal.server.adapter.AuthorizationIdTagAdapter;
import org.connectorio.addons.binding.ocpp.internal.server.adapter.BootRegistrationAdapter;
import org.connectorio.addons.binding.ocpp.internal.server.adapter.DataTransferAdapter;
import org.connectorio.addons.binding.ocpp.internal.server.adapter.MeterValuesConfigAdapter;
import org.connectorio.addons.binding.ocpp.internal.server.adapter.RemoteAuthorizationConfigAdapter;
import org.connectorio.addons.binding.ocpp.internal.server.adapter.RequestListenerAdapter;
import org.connectorio.addons.binding.ocpp.internal.server.adapter.VendorConfigAdapter;
import org.connectorio.addons.binding.ocpp.internal.server.custom.OcularSolarEcoMode;
import org.openhab.core.net.NetworkAddressService;
import org.openhab.core.thing.Bridge;
import org.openhab.core.thing.ChannelUID;
import org.openhab.core.thing.Thing;
import org.openhab.core.thing.ThingStatus;
import org.openhab.core.thing.ThingStatusDetail;
import org.openhab.core.thing.binding.ThingHandler;
import org.openhab.core.thing.binding.ThingHandlerService;
import org.openhab.core.types.Command;
import eu.chargetime.ocpp.model.Confirmation;

public class ServerBridgeHandler extends GenericBridgeHandlerBase<ServerConfig> implements OcppAttendant, OcppSender {

  private final CompositeRequestListener listener = new CompositeRequestListener();

  private NetworkAddressService networkAddressService;
  private OcppServer server;
  private ServerBridgeDispatcherAdapter bridgeHandler;
  private BootRegistrationAdapter bootAdapter;
  private VendorConfigAdapter vendorConfigAdapter;
  private MeterValuesConfigAdapter meterValuesConfigAdapter;
  private RemoteAuthorizationConfigAdapter remoteAuthorizationConfigAdapter;
  /**
   * idTags permitted to charge, from the bridge's {@code tags} parameter. Empty = accept all.
   * Volatile: read via {@link #isTagAuthorized} from charger handlers on OCPP threads while a
   * bridge config edit re-initializes this handler.
   */
  private volatile Set<String> authorizedTags = Collections.emptySet();

  public ServerBridgeHandler(Bridge bridge, NetworkAddressService networkAddressService) {
    super(bridge);
    this.networkAddressService = networkAddressService;
  }

  @Override
  public void initialize() {
    Optional<ServerConfig> thingConfig = getBridgeConfig();
    if (!thingConfig.isPresent()) {
      updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.CONFIGURATION_PENDING, "No configuration found");
      return;
    }

    ServerConfig config = thingConfig.get();
    String address = config.address;
    if (address == null || address.trim().isEmpty()) {
      address = networkAddressService.getPrimaryIpv4HostAddress();
    }
    if (config.port == 0) {
      updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.CONFIGURATION_PENDING, "Port setting missing");
      return;
    }

    Set<String> chargers = set(config.chargers);
    Set<String> tags = set(config.tags);
    authorizedTags = tags;
    Set<String> meterlessChargers = set(config.meterlessChargers);
    int defaultHeartbeatSeconds = config.heartbeat > 0 ? config.heartbeat : 60;

    bootAdapter = new BootRegistrationAdapter(chargers, defaultHeartbeatSeconds);
    bridgeHandler = new ServerBridgeDispatcherAdapter(bootAdapter);
    Deque<ServerCoreEventHandler> eventHandlers = new ConcurrentLinkedDeque<>();
    // 2nd adapter
    eventHandlers.addFirst(new AuthorizationIdTagAdapter(tags));
    // 1st adapter
    eventHandlers.addFirst(bootAdapter);
    eventHandlers.add(bridgeHandler);
    eventHandlers.add(new RequestListenerAdapter(listener));
    // LAST on purpose: catch-all UnknownVendorId for DataTransfer — any vendor-specific handler
    // registered earlier in the chain pre-empts it (first valid confirmation wins).
    eventHandlers.add(new DataTransferAdapter());

    server = new OcppServer(
      address, config.port, bootAdapter, eventHandlers,
      new OcularSolarEcoMode(config.initialOcularEcoMode),
      config.pingInterval
    );
    meterValuesConfigAdapter = new MeterValuesConfigAdapter(bootAdapter, server,
      config.meterValueSampleInterval, config.meterValuesData, config.clockAlignedDataInterval,
      meterlessChargers);
    eventHandlers.addFirst(meterValuesConfigAdapter);
    if (config.disableRemoteTxAuthorization) {
      remoteAuthorizationConfigAdapter = new RemoteAuthorizationConfigAdapter(bootAdapter, server);
      eventHandlers.addFirst(remoteAuthorizationConfigAdapter);
    }
    vendorConfigAdapter = new VendorConfigAdapter(bootAdapter, server, VendorConfigAdapter.parse(config.vendorConfig));
    eventHandlers.addFirst(vendorConfigAdapter);
    server.activate();
    reattachAlreadyInitializedChargers();
    updateStatus(ThingStatus.ONLINE);
  }

  /**
   * A config change on this bridge (e.g. editing vendorConfig) makes openHAB call dispose()+
   * initialize() on THIS handler only — child charger Things are untouched and stay initialized,
   * so childHandlerInitialized() never fires again for them. Without this, the freshly built
   * bridgeHandler starts with an empty charger map, and every inbound StatusNotification/
   * Heartbeat/MeterValues/etc. finds no registered handler: the OCPP library reports the action
   * NotSupported for every already-connected charger until the bundle is restarted. Re-run the
   * same registration childHandlerInitialized() does for any child whose handler already exists.
   */
  private void reattachAlreadyInitializedChargers() {
    for (Thing childThing : getThing().getThings()) {
      ThingHandler childHandler = childThing.getHandler();
      if (childHandler instanceof ChargerThingHandler) {
        childHandlerInitialized(childHandler, childThing);
      }
    }
  }

  @Override
  public void dispose() {
    if (server != null) {
      server.close();
    }
    bridgeHandler = null;
    bootAdapter = null;
    vendorConfigAdapter = null;
    meterValuesConfigAdapter = null;
    remoteAuthorizationConfigAdapter = null;
  }

  private Set<String> set(List<String> config) {
    return Optional.ofNullable(config)
      .map(values -> values.stream()
        .filter(text -> text != null && !text.trim().isEmpty())
        .collect(Collectors.toSet())
      )
      .filter(value -> !value.isEmpty())
      .orElse(Collections.emptySet());
  }

  /**
   * Whether this idTag may charge. Mirrors {@link AuthorizationIdTagAdapter}'s Authorize.req
   * policy (empty {@code tags} = accept everything) so StartTransaction.req — which a charger
   * using local pre-authorization or FreeMode may send without a preceding Authorize.req —
   * enforces the same whitelist instead of rubber-stamping every tag.
   */
  public boolean isTagAuthorized(String idTag) {
    Set<String> tags = authorizedTags;
    return tags.isEmpty() || tags.contains(idTag);
  }

  @Override
  public void handleCommand(ChannelUID channelUID, Command command) {

  }

  @Override
  public void childHandlerInitialized(ThingHandler childHandler, Thing childThing) {
    Object serial = childThing.getConfiguration().get(Thing.PROPERTY_SERIAL_NUMBER);
    if (serial instanceof String && bridgeHandler != null && childHandler instanceof ChargerThingHandler) {
      bridgeHandler.addHandler(new ChargerReference((String) serial), (ChargerThingHandler) childHandler);
      if (bootAdapter != null) {
        Object heartbeatOverride = childThing.getConfiguration().get("heartbeat");
        if (heartbeatOverride instanceof Number) {
          bootAdapter.setHeartbeatInterval((String) serial, ((Number) heartbeatOverride).intValue());
        }
      }
      Object settleOverride = childThing.getConfiguration().get("configSettleSeconds");
      if (settleOverride instanceof Number) {
        long seconds = ((Number) settleOverride).longValue();
        if (vendorConfigAdapter != null) {
          vendorConfigAdapter.setConfigSettleSeconds((String) serial, seconds);
        }
        if (meterValuesConfigAdapter != null) {
          meterValuesConfigAdapter.setConfigSettleSeconds((String) serial, seconds);
        }
        if (remoteAuthorizationConfigAdapter != null) {
          remoteAuthorizationConfigAdapter.setConfigSettleSeconds((String) serial, seconds);
        }
      }
    }
  }

  @Override
  public void childHandlerDisposed(ThingHandler childHandler, Thing childThing) {
    Object serial = childThing.getConfiguration().get(Thing.PROPERTY_SERIAL_NUMBER);
    if (serial instanceof String && bridgeHandler != null) {
      bridgeHandler.removeHandler(new ChargerReference((String) serial));
    }
  }

  @Override
  public Collection<Class<? extends ThingHandlerService>> getServices() {
    return Collections.singleton(OcppChargerDiscoveryService.class);
  }

  @Override
  public <T extends Request> boolean addRequestListener(Class<T> type, OcppRequestListener<T> listener) {
    return this.listener.addRequestListener(type, listener);
  }

  @Override
  public <T extends Request> void removeRequestListener(OcppRequestListener<T> listener) {
    this.listener.removeRequestListener(listener);
  }

  @Override
  public CompletionStage<Confirmation> send(ChargerReference chargerReference, Request request) {
    if (server == null) {
      return CompletableFuture.failedFuture(new NotConnectedException());
    }
    return server.send(chargerReference, request);
  }
}
