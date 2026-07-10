package org.connectorio.addons.binding.ocpp.internal.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import eu.chargetime.ocpp.model.Request;
import eu.chargetime.ocpp.model.core.AuthorizationStatus;
import eu.chargetime.ocpp.model.core.IdTagInfo;
import eu.chargetime.ocpp.model.core.MeterValuesConfirmation;
import eu.chargetime.ocpp.model.core.MeterValuesRequest;
import eu.chargetime.ocpp.model.core.StartTransactionConfirmation;
import eu.chargetime.ocpp.model.core.StartTransactionRequest;
import eu.chargetime.ocpp.model.core.StopTransactionRequest;
import java.util.concurrent.atomic.AtomicInteger;
import org.connectorio.addons.binding.ocpp.internal.OcppRequestListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ChargerConnectorAdapterTest {

  @Mock
  private OcppRequestListener<Request> listener;
  @Mock
  private ConnectorThingHandler connector1;
  @Mock
  private ConnectorThingHandler connector2;

  private ChargerConnectorAdapter adapter;

  @BeforeEach
  void setUp() {
    adapter = new ChargerConnectorAdapter(listener);
  }

  @Test
  void chargerLevelMeterValuesIsAckedNotNull() {
    // connectorId 0 (charger-level, e.g. idle clock-aligned MeterValues) has no connector Thing.
    adapter.addConnector(1, connector1);
    MeterValuesRequest request = org.mockito.Mockito.mock(MeterValuesRequest.class);
    when(request.getConnectorId()).thenReturn(0);

    MeterValuesConfirmation conf = adapter.handleMeterValues(request);

    // must ACK (non-null) so the OCPP layer does not answer the charge point NotSupported
    assertThat(conf).isNotNull();
    verify(connector1, never()).handleMeterValues(any(MeterValuesRequest.class));
  }

  @Test
  void connectorsOfTheSameChargerShareOneTransactionSequence() {
    // when both connectors of one charge point are registered
    adapter.addConnector(1, connector1);
    adapter.addConnector(2, connector2);

    // then they are handed the SAME sequence instance, so their generated ids never collide
    ArgumentCaptor<AtomicInteger> seq1 = ArgumentCaptor.forClass(AtomicInteger.class);
    ArgumentCaptor<AtomicInteger> seq2 = ArgumentCaptor.forClass(AtomicInteger.class);
    verify(connector1).setTransactionSequence(seq1.capture());
    verify(connector2).setTransactionSequence(seq2.capture());
    assertThat(seq1.getValue()).isSameAs(seq2.getValue());
  }

  @Test
  void stopTransactionIsRoutedToTheConnectorThatStartedThatTransaction() {
    adapter.addConnector(1, connector1);
    adapter.addConnector(2, connector2);

    // connector 1 starts transaction id 1, connector 2 starts transaction id 2
    StartTransactionRequest start1 = startOn(1);
    StartTransactionRequest start2 = startOn(2);
    when(connector1.handleStartTransaction(start1)).thenReturn(confirmation(1));
    when(connector2.handleStartTransaction(start2)).thenReturn(confirmation(2));
    adapter.handleStartTransaction(start1);
    adapter.handleStartTransaction(start2);

    // a stop for transaction id 2 must reach connector 2 only
    StopTransactionRequest stop = mockStop(2);
    adapter.handleStopTransaction(stop);

    verify(connector2).handleStopTransaction(stop);
    verify(connector1, never()).handleStopTransaction(any(StopTransactionRequest.class));
  }

  @Test
  void startTransactionOnAConnectorWithNoThingIsStillConfirmed() {
    // OCPP requires a StartTransaction.conf even for a connector the CSMS does not model — an
    // unanswered request becomes a CallError that strands the charger's locally-running
    // transaction. StatusNotification/MeterValues already had this guard; StartTransaction didn't.
    adapter.addConnector(1, connector1);

    StartTransactionRequest start = startOn(7);
    StartTransactionConfirmation conf = adapter.handleStartTransaction(start);

    assertThat(conf).isNotNull();
    assertThat(conf.getIdTagInfo().getStatus()).isEqualTo(AuthorizationStatus.Accepted);
    assertThat(conf.getTransactionId()).isPositive();
    verify(connector1, never()).handleStartTransaction(any(StartTransactionRequest.class));

    // ...and the matching StopTransaction falls through to the generic ACK, not NotSupported.
    assertThat(adapter.handleStopTransaction(mockStop(conf.getTransactionId()))).isNotNull();
  }

  @Test
  void unmappedConnectorConfirmationsDrawDistinctIdsFromTheSharedSequence() {
    adapter.addConnector(1, connector1);

    int first = adapter.handleStartTransaction(startOn(7)).getTransactionId();
    int second = adapter.handleStartTransaction(startOn(8)).getTransactionId();

    // real ids from the charger-wide sequence, not a fixed placeholder — so they can never
    // collide with a modeled connector's transaction
    assertThat(second).isGreaterThan(first);
  }

  @Test
  void aRejectedIdTagIsRefusedWithoutTouchingTheConnector() {
    // A charger with local pre-authorization (or FreeMode) starts the transaction without a
    // preceding Authorize.req — StartTransaction is the only checkpoint where the CSMS can apply
    // its idTag whitelist. Refusal must produce NO connector side effects.
    adapter.addConnector(1, connector1);
    adapter.setTagValidator("friend"::equals);

    StartTransactionRequest strangerStart = startOn(1);
    when(strangerStart.getIdTag()).thenReturn("stranger");
    StartTransactionConfirmation conf = adapter.handleStartTransaction(strangerStart);

    assertThat(conf).isNotNull();
    assertThat(conf.getIdTagInfo().getStatus()).isEqualTo(AuthorizationStatus.Invalid);
    assertThat(conf.getTransactionId()).isPositive(); // schema-required, charger stops with this id
    verify(connector1, never()).handleStartTransaction(any(StartTransactionRequest.class));
  }

  @Test
  void anAuthorizedIdTagIsRoutedNormally() {
    adapter.addConnector(1, connector1);
    adapter.setTagValidator("friend"::equals);

    StartTransactionRequest start = startOn(1);
    when(start.getIdTag()).thenReturn("friend");
    when(connector1.handleStartTransaction(start)).thenReturn(confirmation(5));

    StartTransactionConfirmation conf = adapter.handleStartTransaction(start);

    assertThat(conf.getIdTagInfo().getStatus()).isEqualTo(AuthorizationStatus.Accepted);
    verify(connector1).handleStartTransaction(start);
  }

  @Test
  void stopForATrackedConnectorWhoseHandlerIsGoneStillGetsAcked() {
    adapter.addConnector(1, connector1);
    StartTransactionRequest start = startOn(1);
    when(connector1.handleStartTransaction(start)).thenReturn(confirmation(9));
    adapter.handleStartTransaction(start);

    // the connector Thing is disposed between start and stop
    adapter.removeConnector(1);

    assertThat(adapter.handleStopTransaction(mockStop(9))).isNotNull();
  }

  // lenient: some guard paths legitimately never read these stubs (a rejected idTag never reaches
  // getConnectorId; a stop against an empty transaction map never reads getTransactionId)
  private StartTransactionRequest startOn(int connectorId) {
    StartTransactionRequest request = org.mockito.Mockito.mock(StartTransactionRequest.class);
    org.mockito.Mockito.lenient().when(request.getConnectorId()).thenReturn(connectorId);
    return request;
  }

  private StartTransactionConfirmation confirmation(int transactionId) {
    return new StartTransactionConfirmation(new IdTagInfo(AuthorizationStatus.Accepted), transactionId);
  }

  private StopTransactionRequest mockStop(int transactionId) {
    StopTransactionRequest request = org.mockito.Mockito.mock(StopTransactionRequest.class);
    org.mockito.Mockito.lenient().when(request.getTransactionId()).thenReturn(transactionId);
    return request;
  }
}
