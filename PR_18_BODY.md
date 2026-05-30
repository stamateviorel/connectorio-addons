Chargers persist a hardware current ceiling in a vendor-specific OCPP configuration key (Wallbox uses `chargingALimitConn1`). New `hardwareMaxCurrent` Number:ElectricCurrent channel exposes it: commands are written with `ChangeConfiguration`, `REFRESH` reads back with `GetConfiguration`. The key is set per connector via the new `hardwareMaxCurrentKey` config; the channel is inactive until a key is configured, so chargers without such a key are unaffected.

Stacks on #142. Verified against Wallbox Copper SB / Pulsar Plus (FW 6.7.38) with `chargingALimitConn1`.
