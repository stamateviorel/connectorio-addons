Two standard OCPP 1.6 Core operations the binding didn't surface: Reset and UnlockConnector. Useful for clearing a wedged charger or releasing a stuck cable latch from openHAB without the vendor app.

New momentary Switch channels on the connector: `reset` issues `Reset(Soft)` to the charge point; `lock` issues `UnlockConnector` for this connector (targets the connector id from #142). Both return to OFF once the request settles.

Stacks on #142. Verified against Wallbox Copper SB / Pulsar Plus (FW 6.7.38) and Phoenix Contact CHARX SEC-3xxx (FW 1.9.0).
