OCPP 1.6 has no dedicated pause command — the conventional way to pause a charging EV without ending the transaction is to apply a 0 A limit via `SetChargingProfile`, then restore the previous limit to resume. Today a ConnectorIO user has to script that against the `chargeLimit` channel.

New `pause` Switch channel on the connector: ON applies a 0 A `SetChargingProfile`, OFF restores the last non-zero limit the handler last saw (tracked in `ChargeLimitCommandHandler` so resume returns to whatever the user/EMS last requested).

Verified against Wallbox Copper SB / Pulsar Plus (FW 6.7.38): pause → SuspendedEVSE, resume → Charging at the prior current.
