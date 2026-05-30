package org.connectorio.addons.binding.ocpp.internal.handler;

import eu.chargetime.ocpp.model.core.ChargingProfile;
import eu.chargetime.ocpp.model.core.ChargingProfileKindType;
import eu.chargetime.ocpp.model.core.ChargingProfilePurposeType;
import eu.chargetime.ocpp.model.core.ChargingRateUnitType;
import eu.chargetime.ocpp.model.core.ChargingSchedule;
import eu.chargetime.ocpp.model.core.ChargingSchedulePeriod;
import eu.chargetime.ocpp.model.smartcharging.SetChargingProfileRequest;
import org.connectorio.addons.binding.ocpp.internal.OcppSender;
import org.connectorio.addons.binding.ocpp.internal.server.ChargerReference;
import org.openhab.core.library.types.DecimalType;
import org.openhab.core.library.types.QuantityType;
import org.openhab.core.types.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChargeLimitCommandHandler {
    private final Logger logger = LoggerFactory.getLogger(ChargeLimitCommandHandler.class);

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
        sendChargingProfile(limit, context);
    }

    private void sendChargingProfile(double limit, ConnectorCommandContext context) {
        OcppSender ocppSender = context.getOcppSender();
        String chargerSerialNumber = context.getChargerSerialNumber();
        if (ocppSender == null || chargerSerialNumber == null) {
            logger.warn("OcppSender or charger serial not set. Cannot send charging profile.");
            return;
        }

        ChargerReference chargerRef = new ChargerReference(chargerSerialNumber);

        // Create charging profile with the specified limit
        ChargingSchedulePeriod period = new ChargingSchedulePeriod(0, limit);
        ChargingSchedule schedule = new ChargingSchedule(
            ChargingRateUnitType.A,
            new ChargingSchedulePeriod[]{ period }
        );

        ChargingProfile profile = new ChargingProfile();
        profile.setChargingProfileId(1);
        profile.setStackLevel(0);
        profile.setChargingProfileKind(ChargingProfileKindType.Relative);
        profile.setChargingSchedule(schedule);

        // A limit applied while a transaction is running must target that transaction with a
        // TxProfile carrying its transactionId. Per OCPP 1.6 Smart Charging a TxDefaultProfile
        // sets the default for *future* transactions, so spec-strict charge points may defer a
        // mid-session TxDefaultProfile to the next transaction instead of acting on the running
        // one. Fall back to TxDefaultProfile only when no transaction is active, which seeds the
        // limit for the next session.
        Integer transactionId = context.getCurrentTransactionId();
        if (transactionId != null) {
            profile.setChargingProfilePurpose(ChargingProfilePurposeType.TxProfile);
            profile.setTransactionId(transactionId);
        } else {
            profile.setChargingProfilePurpose(ChargingProfilePurposeType.TxDefaultProfile);
        }

        SetChargingProfileRequest setProfileRequest = new SetChargingProfileRequest(1, profile);

        ocppSender.send(chargerRef, setProfileRequest).whenComplete((confirmation, throwable) -> {
            if (throwable != null) {
                logger.warn("Failed to send SetChargingProfile with limit {}", limit, throwable);
            } else {
                logger.info("SetChargingProfile with limit {} sent successfully: {}", limit, confirmation);
            }
        });
    }
}
