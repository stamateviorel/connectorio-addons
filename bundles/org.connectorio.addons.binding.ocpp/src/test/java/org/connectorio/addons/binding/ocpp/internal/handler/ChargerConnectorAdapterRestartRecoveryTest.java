package org.connectorio.addons.binding.ocpp.internal.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import eu.chargetime.ocpp.model.Request;
import eu.chargetime.ocpp.model.core.StopTransactionConfirmation;
import eu.chargetime.ocpp.model.core.StopTransactionRequest;
import java.util.concurrent.atomic.AtomicInteger;
import org.connectorio.addons.binding.ocpp.internal.OcppRequestListener;
import org.junit.jupiter.api.Test;

/**
 * After a binding restart, a connector restores its in-flight transaction id from persisted state.
 * Re-registering it must rebuild the adapter's reverse-routing map so the charger's StopTransaction
 * (which carries no connectorId) still reaches the right connector — and the shared id sequence must
 * skip past the still-open id so a new transaction cannot reuse it.
 */
class ChargerConnectorAdapterRestartRecoveryTest {

  @SuppressWarnings("unchecked")
  private final OcppRequestListener<Request> listener = mock(OcppRequestListener.class);
  private final ConnectorThingHandler connector1 = mock(ConnectorThingHandler.class);
  private final ChargerConnectorAdapter adapter = new ChargerConnectorAdapter(listener);

  @Test
  void restoredTransactionStillRoutesItsStopAfterRestart() {
    when(connector1.handleStopTransaction(any())).thenReturn(new StopTransactionConfirmation());
    // Connector came back from restart already holding its persisted transaction id.
    when(connector1.getCurrentTransactionId()).thenReturn(7);

    adapter.addConnector(1, connector1);

    StopTransactionRequest stop = new StopTransactionRequest();
    stop.setTransactionId(7);
    adapter.handleStopTransaction(stop);

    verify(connector1, times(1)).handleStopTransaction(stop);
  }

  @Test
  void sharedSequenceSkipsPastRestoredId() {
    when(connector1.getCurrentTransactionId()).thenReturn(7);
    AtomicInteger sequenceProbe = new AtomicInteger();
    // Capture the shared sequence handed to the connector so we can assert it advanced past 7.
    org.mockito.Mockito.doAnswer(invocation -> {
      sequenceProbe.set(((AtomicInteger) invocation.getArgument(0)).get());
      return null;
    }).when(connector1).setTransactionSequence(any());

    adapter.addConnector(1, connector1);

    assertThat(sequenceProbe.get()).isLessThanOrEqualTo(7); // sequence handed over before the bump
    // After registration the shared sequence must be > 7 so the next generated id cannot collide.
    // Re-add via a second connector reading the same sequence to observe the advanced value.
    ConnectorThingHandler connector2 = mock(ConnectorThingHandler.class);
    when(connector2.getCurrentTransactionId()).thenReturn(null);
    AtomicInteger seen = new AtomicInteger();
    org.mockito.Mockito.doAnswer(invocation -> {
      seen.set(((AtomicInteger) invocation.getArgument(0)).get());
      return null;
    }).when(connector2).setTransactionSequence(any());
    adapter.addConnector(2, connector2);

    assertThat(seen.get()).isGreaterThanOrEqualTo(8);
  }

  @Test
  void noRestoredTransactionLeavesMapEmpty() {
    when(connector1.getCurrentTransactionId()).thenReturn(null);
    adapter.addConnector(1, connector1);

    StopTransactionRequest stop = new StopTransactionRequest();
    stop.setTransactionId(7);
    adapter.handleStopTransaction(stop);

    verify(connector1, never()).handleStopTransaction(any());
  }
}
