OCPP 1.6 has no plug-presence message — automation that needs to know whether an EV is plugged in has to interpret the raw `chargePointStatus` string itself. New read-only `cableConnected` Switch channel derives it: ON for Preparing/Charging/SuspendedEV/SuspendedEVSE/Finishing, OFF for Available/Unavailable/Faulted/Reserved. Updated on every StatusNotification.

Verified against Wallbox Copper SB / Pulsar Plus (FW 6.7.38) and Phoenix Contact CHARX SEC-3xxx (FW 1.9.0): plug → ON at Preparing, unplug → OFF at Available.
