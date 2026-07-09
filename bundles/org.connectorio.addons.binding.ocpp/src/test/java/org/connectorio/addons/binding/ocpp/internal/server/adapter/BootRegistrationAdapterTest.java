package org.connectorio.addons.binding.ocpp.internal.server.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import eu.chargetime.ocpp.model.core.BootNotificationConfirmation;
import eu.chargetime.ocpp.model.core.BootNotificationRequest;
import java.util.Set;
import java.util.UUID;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;
import org.junit.jupiter.api.Test;

/**
 * Guards the reconnect-routing fix: a charger that reconnects on a new session UUID must not leave
 * a stale same-serial entry behind, or outbound CALLs could resolve to the dead session
 * (IllegalStateException "connect() must be called first").
 */
class BootRegistrationAdapterTest {

  @Test
  void reconnectEvictsStaleSameSerialSession() {
    BootRegistrationAdapter registry = new BootRegistrationAdapter(Set.of());
    ChargerReference charx = new ChargerReference("charx");
    UUID first = UUID.randomUUID();
    UUID second = UUID.randomUUID();

    registry.registerSession(first, charx);
    assertThat(registry.getSession(charx)).isEqualTo(first);

    // Reconnect: fresh UUID, same serial. The stale 'first' must be evicted so outbound CALLs
    // always resolve to the live 'second' session, never the dead one.
    registry.registerSession(second, charx);

    assertThat(registry.getSession(charx)).isEqualTo(second);
    assertThat(registry.getCharger(first)).isNull();
    assertThat(registry.getCharger(second)).isEqualTo(charx);
  }

  @Test
  void differentSerialsCoexist() {
    BootRegistrationAdapter registry = new BootRegistrationAdapter(Set.of());
    UUID a = UUID.randomUUID();
    UUID b = UUID.randomUUID();

    registry.registerSession(a, new ChargerReference("charx"));
    registry.registerSession(b, new ChargerReference("wallbox"));

    assertThat(registry.getSession(new ChargerReference("charx"))).isEqualTo(a);
    assertThat(registry.getSession(new ChargerReference("wallbox"))).isEqualTo(b);
  }

  @Test
  void reRegisteringSameUuidIsIdempotent() {
    BootRegistrationAdapter registry = new BootRegistrationAdapter(Set.of());
    ChargerReference charx = new ChargerReference("charx");
    UUID session = UUID.randomUUID();

    registry.registerSession(session, charx);
    registry.registerSession(session, charx);

    assertThat(registry.getSession(charx)).isEqualTo(session);
    assertThat(registry.getCharger(session)).isEqualTo(charx);
  }

  @Test
  void bootNotificationUsesTheConfiguredDefaultInterval() {
    BootRegistrationAdapter registry = new BootRegistrationAdapter(Set.of(), 300);
    UUID session = UUID.randomUUID();
    registry.registerSession(session, new ChargerReference("charx"));

    BootNotificationConfirmation confirmation =
        registry.handleBootNotificationRequest(session, new BootNotificationRequest());

    assertThat(confirmation.getInterval()).isEqualTo(300);
  }

  @Test
  void perChargerHeartbeatOverrideWinsOverTheDefault() {
    BootRegistrationAdapter registry = new BootRegistrationAdapter(Set.of(), 60);
    UUID charxSession = UUID.randomUUID();
    UUID wallboxSession = UUID.randomUUID();
    registry.registerSession(charxSession, new ChargerReference("charx"));
    registry.registerSession(wallboxSession, new ChargerReference("wallbox"));

    registry.setHeartbeatInterval("charx", 300);

    assertThat(registry.handleBootNotificationRequest(charxSession, new BootNotificationRequest()).getInterval())
        .isEqualTo(300);
    assertThat(registry.handleBootNotificationRequest(wallboxSession, new BootNotificationRequest()).getInterval())
        .isEqualTo(60);
  }

  @Test
  void zeroOrNegativeDefaultFallsBackToSixty() {
    BootRegistrationAdapter registry = new BootRegistrationAdapter(Set.of(), 0);
    UUID session = UUID.randomUUID();
    registry.registerSession(session, new ChargerReference("charx"));

    assertThat(registry.handleBootNotificationRequest(session, new BootNotificationRequest()).getInterval())
        .isEqualTo(60);
  }
}
