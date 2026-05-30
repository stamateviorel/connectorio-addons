package org.connectorio.addons.binding.ocpp.internal.handler;

import eu.chargetime.ocpp.model.core.ChargingProfile;
import eu.chargetime.ocpp.model.core.ChargingProfileKindType;
import eu.chargetime.ocpp.model.core.ChargingProfilePurposeType;
import eu.chargetime.ocpp.model.core.ChargingRateUnitType;
import eu.chargetime.ocpp.model.core.ChargingSchedule;
import eu.chargetime.ocpp.model.core.ChargingSchedulePeriod;
import eu.chargetime.ocpp.model.smartcharging.SetChargingProfileRequest;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;
import org.connectorio.addons.binding.ocpp.internal.server.SetChargingProfileCoalescer;
import org.openhab.core.library.types.DecimalType;
import org.openhab.core.library.types.QuantityType;
import org.openhab.core.types.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChargeLimitCommandHandler {
    private final Logger logger = LoggerFactory.getLogger(ChargeLimitCommandHandler.class);

    // One coalescer per handler instance — handlers are created per connector.
    private SetChargingProfileCoalescer coalescer;

    public void handle(Command command, ConnectorCommandContext context) {
        double limit;
        if (command instanceof DecimalType) {
            limit = ((DecimalType) command).doubleValue();
        } else if (command instanceof QuantityType) {
            limit = ((QuantityType<?>) command).doubleValue();
        } else {
            logger.warn("Unsupported command type for chargeLimit: {}", command.getClass());
            return;
        }
        if (context.getOcppSender() == null || context.getChargerSerialNumber() == null) {
            logger.warn("OcppSender or charger serial not set. Cannot send charging profile.");
            return;
        }
        coalescer(context).submit((int) Math.round(limit));
    }

    private synchronized SetChargingProfileCoalescer coalescer(ConnectorCommandContext context) {
        if (coalescer == null) {
            coalescer = new SetChargingProfileCoalescer(
                context.getProfileMinIntervalMs(),
                System::currentTimeMillis,
                wire -> sendChargingProfile(wire, context),
                (task, delayMs) -> context.getScheduler().schedule(task, delayMs, TimeUnit.MILLISECONDS));
        }
        return coalescer;
    }

    private CompletionStage<?> sendChargingProfile(int limit, ConnectorCommandContext context) {
        ChargerReference chargerRef = new ChargerReference(context.getChargerSerialNumber());

        ChargingSchedulePeriod period = new ChargingSchedulePeriod(0, (double) limit);
        ChargingSchedule schedule = new ChargingSchedule(
            ChargingRateUnitType.A,
            new ChargingSchedulePeriod[]{ period }
        );

        ChargingProfile profile = new ChargingProfile();
        profile.setChargingProfileId(1);
        profile.setStackLevel(0);
        profile.setChargingProfilePurpose(ChargingProfilePurposeType.TxDefaultProfile);
        profile.setChargingProfileKind(ChargingProfileKindType.Relative);
        profile.setChargingSchedule(schedule);

        SetChargingProfileRequest setProfileRequest = new SetChargingProfileRequest(1, profile);

        return context.getOcppSender().send(chargerRef, setProfileRequest).whenComplete((confirmation, throwable) -> {
            if (throwable != null) {
                logger.warn("Failed to send SetChargingProfile with limit {}", limit, throwable);
            } else {
                logger.info("SetChargingProfile with limit {} sent successfully: {}", limit, confirmation);
            }
        });
    }
}
