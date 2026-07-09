package org.connectorio.addons.binding.ocpp.internal.server.adapter;

import eu.chargetime.ocpp.model.core.BootNotificationConfirmation;
import eu.chargetime.ocpp.model.core.BootNotificationRequest;
import eu.chargetime.ocpp.model.core.RegistrationStatus;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;
import org.connectorio.addons.binding.ocpp.internal.server.OcppChargerSessionRegistry;

public class BootRegistrationAdapter extends CoreEventHandlerAdapter implements
  OcppChargerSessionRegistry {

  private final Map<UUID, ChargerReference> registrations = new ConcurrentHashMap<>();

  /**
   * Per-charger serial override of the heartbeat interval sent in BootNotificationConfirmation.
   * Populated from each charger Thing's own {@code heartbeat} config (see ChargerConfig); falls
   * back to {@link #defaultIntervalSeconds} for any charger without its own override.
   */
  private final Map<String, Integer> heartbeatIntervals = new ConcurrentHashMap<>();

  private final Set<String> identifiers;
  private final int defaultIntervalSeconds;

  public BootRegistrationAdapter(Set<String> identifiers) {
    this(identifiers, 60);
  }

  public BootRegistrationAdapter(Set<String> identifiers, int defaultIntervalSeconds) {
    this.identifiers = identifiers;
    this.defaultIntervalSeconds = defaultIntervalSeconds > 0 ? defaultIntervalSeconds : 60;
  }

  public void setHeartbeatInterval(String serial, int seconds) {
    if (serial != null && seconds > 0) {
      heartbeatIntervals.put(serial, seconds);
    }
  }

  @Override
  public void registerSession(UUID session, ChargerReference chargerReference) {
      // A reconnecting charger arrives on a fresh session UUID while its previous
      // session's close may not yet have been reported by the WebSocket layer
      // (half-open socket, fast reconnect, or bundle reload). If a stale same-serial
      // entry survives, getSession() can resolve outbound CALLs to the dead UUID,
      // whose socket is closed -> IllegalStateException "connect() must be called
      // first" (inbound still works on the live socket, so the symptom is
      // send-only). Evict any prior entry for this serial so the newest connection
      // always wins and reconnects self-heal without a charger reboot.
      registrations.entrySet().removeIf(
          entry -> !entry.getKey().equals(session) && entry.getValue().equals(chargerReference));
      registrations.put(session, chargerReference);
  }

  @Override
  public BootNotificationConfirmation handleBootNotificationRequest(UUID sessionIndex, BootNotificationRequest request) {
    ZonedDateTime time = ZonedDateTime.now();
    // Session already registered in newSession()
    
    ChargerReference registered = registrations.get(sessionIndex);
    if (identifiers.isEmpty() || (registered != null && identifiers.contains(registered.getSerial()))) {
      int interval = registered != null
          ? heartbeatIntervals.getOrDefault(registered.getSerial(), defaultIntervalSeconds)
          : defaultIntervalSeconds;
      return new BootNotificationConfirmation(time, interval, RegistrationStatus.Accepted);
    }

    // keep charger connected, but not active
    return new BootNotificationConfirmation(time, 300, RegistrationStatus.Pending);
  }

  @Override
  public UUID getSession(ChargerReference chargerReference) {
    for (Map.Entry<UUID, ChargerReference> entry : registrations.entrySet()) {
      if (entry.getValue().equals(chargerReference)) {
        return entry.getKey();
      }
    }
    return null;
  }

  @Override
  public ChargerReference removeSession(UUID session) {
    return registrations.remove(session);
  }

  @Override
  public ChargerReference getCharger(UUID sessionIndex) {
    return registrations.get(sessionIndex);
  }
}
