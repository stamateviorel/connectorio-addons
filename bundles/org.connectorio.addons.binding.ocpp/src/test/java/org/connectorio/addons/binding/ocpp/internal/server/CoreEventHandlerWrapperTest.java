package org.connectorio.addons.binding.ocpp.internal.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import eu.chargetime.ocpp.feature.profile.ServerCoreEventHandler;
import eu.chargetime.ocpp.model.core.HeartbeatRequest;
import eu.chargetime.ocpp.model.core.MeterValuesRequest;
import eu.chargetime.ocpp.model.core.StatusNotificationRequest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Every inbound OCPP message must notify the liveness watchdog with its session UUID — this is what
 * lets the server reap a charger whose OCPP stack froze while the WebSocket transport stayed up.
 */
class CoreEventHandlerWrapperTest {

  private final ServerCoreEventHandler delegate = mock(ServerCoreEventHandler.class);
  private final Deque<ServerCoreEventHandler> handlers = new ArrayDeque<>(List.of(delegate));
  private final List<UUID> touched = new ArrayList<>();
  private final CoreEventHandlerWrapper wrapper = new CoreEventHandlerWrapper(handlers, touched::add);

  @Test
  void heartbeatStampsLiveness() {
    UUID session = UUID.randomUUID();
    wrapper.handleHeartbeatRequest(session, new HeartbeatRequest());
    assertThat(touched).containsExactly(session);
  }

  @Test
  void everyInboundMessageStampsLiveness() {
    UUID session = UUID.randomUUID();
    wrapper.handleHeartbeatRequest(session, new HeartbeatRequest());
    wrapper.handleStatusNotificationRequest(session, new StatusNotificationRequest());
    wrapper.handleMeterValuesRequest(session, new MeterValuesRequest());
    assertThat(touched).containsExactly(session, session, session);
  }
}
