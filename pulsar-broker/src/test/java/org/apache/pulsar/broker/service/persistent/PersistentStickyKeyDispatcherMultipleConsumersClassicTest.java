/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.persistent;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.plugin.EntryFilterProvider;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentStickyKeyDispatcherMultipleConsumersClassicTest {

    private PulsarService pulsarMock;
    private BrokerService brokerMock;
    private ManagedCursorImpl cursorMock;
    private Consumer consumerMock;
    private PersistentTopic topicMock;
    private PersistentSubscription subscriptionMock;
    private ServiceConfiguration configMock;
    private ChannelPromise channelMock;
    private OrderedExecutor orderedExecutor;

    private PersistentStickyKeyDispatcherMultipleConsumersClassic persistentDispatcher;

    final String topicName = "persistent://public/default/testTopic";
    final String subscriptionName = "testSubscription";

    @BeforeMethod
    public void setup() throws Exception {
        configMock = mock(ServiceConfiguration.class);
        doReturn(true).when(configMock).isSubscriptionRedeliveryTrackerEnabled();
        doReturn(100).when(configMock).getDispatcherMaxReadBatchSize();
        doReturn(true).when(configMock).isSubscriptionKeySharedUseConsistentHashing();
        doReturn(1).when(configMock).getSubscriptionKeySharedConsistentHashingReplicaPoints();
        doReturn(true).when(configMock).isDispatcherDispatchMessagesInSubscriptionThread();
        doReturn(false).when(configMock).isAllowOverrideEntryFilters();

        pulsarMock = mock(PulsarService.class);
        doReturn(configMock).when(pulsarMock).getConfiguration();

        EntryFilterProvider mockEntryFilterProvider = mock(EntryFilterProvider.class);
        when(mockEntryFilterProvider.getBrokerEntryFilters()).thenReturn(Collections.emptyList());

        brokerMock = mock(BrokerService.class);
        doReturn(pulsarMock).when(brokerMock).pulsar();
        when(brokerMock.getEntryFilterProvider()).thenReturn(mockEntryFilterProvider);

        HierarchyTopicPolicies topicPolicies = new HierarchyTopicPolicies();
        topicPolicies.getMaxConsumersPerSubscription().updateBrokerValue(0);

        orderedExecutor = OrderedExecutor.newBuilder().build();
        doReturn(orderedExecutor).when(brokerMock).getTopicOrderedExecutor();

        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        doReturn(eventLoopGroup).when(brokerMock).executor();
        doAnswer(invocation -> {
            orderedExecutor.execute(((Runnable)invocation.getArguments()[0]));
            return null;
        }).when(eventLoopGroup).execute(any(Runnable.class));

        topicMock = mock(PersistentTopic.class);
        doReturn(brokerMock).when(topicMock).getBrokerService();
        doReturn(topicName).when(topicMock).getName();
        doReturn(topicPolicies).when(topicMock).getHierarchyTopicPolicies();

        cursorMock = mock(ManagedCursorImpl.class);
        doReturn(null).when(cursorMock).getLastIndividualDeletedRange();
        doReturn(subscriptionName).when(cursorMock).getName();

        consumerMock = mock(Consumer.class);
        channelMock = mock(ChannelPromise.class);
        doReturn("consumer1").when(consumerMock).consumerName();
        doReturn(1000).when(consumerMock).getAvailablePermits();
        doReturn(true).when(consumerMock).isWritable();
        doReturn(channelMock).when(consumerMock).sendMessages(
                anyList(),
                any(EntryBatchSizes.class),
                any(EntryBatchIndexesAcks.class),
                anyInt(),
                anyLong(),
                anyLong(),
                any(RedeliveryTracker.class)
        );

        subscriptionMock = mock(PersistentSubscription.class);
        when(subscriptionMock.getTopic()).thenReturn(topicMock);
        persistentDispatcher = new PersistentStickyKeyDispatcherMultipleConsumersClassic(
                topicMock, cursorMock, subscriptionMock, configMock,
                new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT));
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (persistentDispatcher != null && !persistentDispatcher.isClosed()) {
            persistentDispatcher.close();
        }
        if (orderedExecutor != null) {
            orderedExecutor.shutdownNow();
            orderedExecutor = null;
        }
    }

    @Test(timeOut = 10000)
    public void testAddConsumerWhenClosed() throws Exception {
        persistentDispatcher.close().get();
        Consumer consumer = mock(Consumer.class);
        persistentDispatcher.addConsumer(consumer);
        verify(consumer, times(1)).disconnect();
        assertEquals(0, persistentDispatcher.getConsumers().size());
        assertTrue(persistentDispatcher.getSelector().getConsumerKeyHashRanges().isEmpty());
    }

    @Test
    public void testSortRecentlyJoinedConsumersIfNeeded() throws Exception {
        PersistentStickyKeyDispatcherMultipleConsumersClassic persistentDispatcher =
                new PersistentStickyKeyDispatcherMultipleConsumersClassic(
                topicMock, cursorMock, subscriptionMock, configMock,
                new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT));

        Consumer consumer0 = mock(Consumer.class);
        when(consumer0.consumerName()).thenReturn("c0-1");
        Consumer consumer1 = mock(Consumer.class);
        when(consumer1.consumerName()).thenReturn("c1");
        Consumer consumer2 = mock(Consumer.class);
        when(consumer2.consumerName()).thenReturn("c2");
        Consumer consumer3 = mock(Consumer.class);
        when(consumer3.consumerName()).thenReturn("c3");
        Consumer consumer4 = mock(Consumer.class);
        when(consumer4.consumerName()).thenReturn("c4");
        Consumer consumer5 = mock(Consumer.class);
        when(consumer5.consumerName()).thenReturn("c5");
        Consumer consumer6 = mock(Consumer.class);
        when(consumer6.consumerName()).thenReturn("c6");

        when(cursorMock.getNumberOfEntriesSinceFirstNotAckedMessage()).thenReturn(100L);
        when(cursorMock.getMarkDeletedPosition()).thenReturn(PositionFactory.create(-1, -1));

        when(cursorMock.getReadPosition()).thenReturn(PositionFactory.create(0, 0));
        persistentDispatcher.addConsumer(consumer0).join();

        when(cursorMock.getReadPosition()).thenReturn(PositionFactory.create(4, 1));
        persistentDispatcher.addConsumer(consumer1).join();

        when(cursorMock.getReadPosition()).thenReturn(PositionFactory.create(5, 2));
        persistentDispatcher.addConsumer(consumer2).join();

        when(cursorMock.getReadPosition()).thenReturn(PositionFactory.create(5, 1));
        persistentDispatcher.addConsumer(consumer3).join();

        when(cursorMock.getReadPosition()).thenReturn(PositionFactory.create(5, 3));
        persistentDispatcher.addConsumer(consumer4).join();

        when(cursorMock.getReadPosition()).thenReturn(PositionFactory.create(4, 2));
        persistentDispatcher.addConsumer(consumer5).join();

        when(cursorMock.getReadPosition()).thenReturn(PositionFactory.create(6, 1));
        persistentDispatcher.addConsumer(consumer6).join();

        assertEquals(persistentDispatcher.getRecentlyJoinedConsumers().size(), 6);

        Iterator<Map.Entry<Consumer, Position>> itr
                = persistentDispatcher.getRecentlyJoinedConsumers().entrySet().iterator();

        Map.Entry<Consumer, Position> entry1 = itr.next();
        assertEquals(entry1.getValue(), PositionFactory.create(4, 1));
        assertEquals(entry1.getKey(), consumer1);

        Map.Entry<Consumer, Position> entry2 = itr.next();
        assertEquals(entry2.getValue(), PositionFactory.create(4, 2));
        assertEquals(entry2.getKey(), consumer5);

        Map.Entry<Consumer, Position> entry3 = itr.next();
        assertEquals(entry3.getValue(), PositionFactory.create(5, 1));
        assertEquals(entry3.getKey(), consumer3);

        Map.Entry<Consumer, Position> entry4 = itr.next();
        assertEquals(entry4.getValue(), PositionFactory.create(5, 2));
        assertEquals(entry4.getKey(), consumer2);

        Map.Entry<Consumer, Position> entry5 = itr.next();
        assertEquals(entry5.getValue(), PositionFactory.create(5, 3));
        assertEquals(entry5.getKey(), consumer4);

        Map.Entry<Consumer, Position> entry6 = itr.next();
        assertEquals(entry6.getValue(), PositionFactory.create(6, 1));
        assertEquals(entry6.getKey(), consumer6);

        // cleanup.
        persistentDispatcher.close();
    }

    @Test
    public void testSendMarkerMessage() {
        try {
            persistentDispatcher.addConsumer(consumerMock);
            persistentDispatcher.consumerFlow(consumerMock, 1000);
        } catch (Exception e) {
            fail("Failed to add mock consumer", e);
        }

        List<Entry> entries = new ArrayList<>();
        ByteBuf markerMessage = Markers.newReplicatedSubscriptionsSnapshotRequest("testSnapshotId", "testSourceCluster");
        entries.add(EntryImpl.create(1, 1, markerMessage));
        entries.add(EntryImpl.create(1, 2, createMessage("message1", 1)));
        entries.add(EntryImpl.create(1, 3, createMessage("message2", 2)));
        entries.add(EntryImpl.create(1, 4, createMessage("message3", 3)));
        entries.add(EntryImpl.create(1, 5, createMessage("message4", 4)));
        entries.add(EntryImpl.create(1, 6, createMessage("message5", 5)));

        try {
            persistentDispatcher.readEntriesComplete(entries, PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Normal);
        } catch (Exception e) {
            fail("Failed to readEntriesComplete.", e);
        }

        Awaitility.await().untilAsserted(() -> {
            ArgumentCaptor<Integer> totalMessagesCaptor = ArgumentCaptor.forClass(Integer.class);
            verify(consumerMock, times(1)).sendMessages(
                    anyList(),
                    any(EntryBatchSizes.class),
                    any(EntryBatchIndexesAcks.class),
                    totalMessagesCaptor.capture(),
                    anyLong(),
                    anyLong(),
                    any(RedeliveryTracker.class)
            );

            List<Integer> allTotalMessagesCaptor = totalMessagesCaptor.getAllValues();
            Assert.assertEquals(allTotalMessagesCaptor.get(0).intValue(), 5);
        });
    }

    @Test(timeOut = 10000)
    public void testSendMessage() {
        KeySharedMeta keySharedMeta = new KeySharedMeta().setKeySharedMode(KeySharedMode.STICKY);
        PersistentStickyKeyDispatcherMultipleConsumersClassic
                persistentDispatcher = new PersistentStickyKeyDispatcherMultipleConsumersClassic(
                topicMock, cursorMock, subscriptionMock, configMock, keySharedMeta);
        try {
            keySharedMeta.addHashRange()
                    .setStart(0)
                    .setEnd(9);

            Consumer consumerMock = mock(Consumer.class);
            doReturn(keySharedMeta).when(consumerMock).getKeySharedMeta();
            persistentDispatcher.addConsumer(consumerMock);
            persistentDispatcher.consumerFlow(consumerMock, 1000);
        } catch (Exception e) {
            fail("Failed to add mock consumer", e);
        }

        List<Entry> entries = new ArrayList<>();
        entries.add(EntryImpl.create(1, 1, createMessage("message1", 1)));
        entries.add(EntryImpl.create(1, 2, createMessage("message2", 2)));

        try {
            //Should success,see issue #8960
            persistentDispatcher.readEntriesComplete(entries, PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Normal);
        } catch (Exception e) {
            fail("Failed to readEntriesComplete.", e);
        }
    }

    @Test
    public void testSkipRedeliverTemporally() {
        final Consumer slowConsumerMock = mock(Consumer.class);
        final ChannelPromise slowChannelMock = mock(ChannelPromise.class);
        // add entries to redeliver and read target
        final List<Entry> redeliverEntries = new ArrayList<>();
        redeliverEntries.add(EntryImpl.create(1, 1, createMessage("message1", 1, "key1")));
        final List<Entry> readEntries = new ArrayList<>();
        readEntries.add(EntryImpl.create(1, 2, createMessage("message2", 2, "key1")));
        readEntries.add(EntryImpl.create(1, 3, createMessage("message3", 3, "key2")));

        try {
            Field totalAvailablePermitsField = PersistentDispatcherMultipleConsumersClassic.class.getDeclaredField("totalAvailablePermits");
            totalAvailablePermitsField.setAccessible(true);
            totalAvailablePermitsField.set(persistentDispatcher, 1000);

            doAnswer(invocationOnMock -> {
                ((PersistentStickyKeyDispatcherMultipleConsumersClassic) invocationOnMock.getArgument(2))
                        .readEntriesComplete(readEntries, PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Normal);
                return null;
            }).when(cursorMock).asyncReadEntriesOrWait(
                    anyInt(), anyLong(), any(PersistentStickyKeyDispatcherMultipleConsumersClassic.class),
                    eq(PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Normal), any());
        } catch (Exception e) {
            fail("Failed to set to field", e);
        }

        // Create 2Consumers
        try {
            doReturn("consumer2").when(slowConsumerMock).consumerName();
            // Change slowConsumer availablePermits to 0 and back to normal
            when(slowConsumerMock.getAvailablePermits())
                    .thenReturn(0)
                    .thenReturn(1);
            doReturn(true).when(slowConsumerMock).isWritable();
            doReturn(slowChannelMock).when(slowConsumerMock).sendMessages(
                    anyList(),
                    any(EntryBatchSizes.class),
                    any(EntryBatchIndexesAcks.class),
                    anyInt(),
                    anyLong(),
                    anyLong(),
                    any(RedeliveryTracker.class)
            );

            persistentDispatcher.addConsumer(consumerMock);
            persistentDispatcher.addConsumer(slowConsumerMock);
        } catch (Exception e) {
            fail("Failed to add mock consumer", e);
        }

        // run PersistentStickyKeyDispatcherMultipleConsumers#sendMessagesToConsumers
        // run readMoreEntries internally (and skip internally)
        // Change slowConsumer availablePermits to 1
        // run PersistentStickyKeyDispatcherMultipleConsumers#sendMessagesToConsumers internally
        // and then stop to dispatch to slowConsumer
        if (persistentDispatcher.sendMessagesToConsumers(PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Normal,
                redeliverEntries, true)) {
            persistentDispatcher.readMoreEntriesAsync();
        }

        Awaitility.await().untilAsserted(() -> {
            verify(consumerMock, times(1)).sendMessages(
                    argThat(arg -> {
                        assertEquals(arg.size(), 1);
                        Entry entry = arg.get(0);
                        assertEquals(entry.getLedgerId(), 1);
                        assertEquals(entry.getEntryId(), 3);
                        return true;
                    }),
                    any(EntryBatchSizes.class),
                    any(EntryBatchIndexesAcks.class),
                    anyInt(),
                    anyLong(),
                    anyLong(),
                    any(RedeliveryTracker.class)
            );
        });
        verify(slowConsumerMock, times(0)).sendMessages(
                anyList(),
                any(EntryBatchSizes.class),
                any(EntryBatchIndexesAcks.class),
                anyInt(),
                anyLong(),
                anyLong(),
                any(RedeliveryTracker.class)
        );
    }

    @Test(timeOut = 30000)
    public void testMessageRedelivery() throws Exception {
        final Queue<Position> actualEntriesToConsumer1 = new ConcurrentLinkedQueue<>();
        final Queue<Position> actualEntriesToConsumer2 = new ConcurrentLinkedQueue<>();

        final Queue<Position> expectedEntriesToConsumer1 = new ConcurrentLinkedQueue<>();
        expectedEntriesToConsumer1.add(PositionFactory.create(1, 1));
        final Queue<Position> expectedEntriesToConsumer2 = new ConcurrentLinkedQueue<>();
        expectedEntriesToConsumer2.add(PositionFactory.create(1, 2));
        expectedEntriesToConsumer2.add(PositionFactory.create(1, 3));

        final AtomicInteger remainingEntriesNum = new AtomicInteger(
                expectedEntriesToConsumer1.size() + expectedEntriesToConsumer2.size());

        // Messages with key1 are routed to consumer1 and messages with key2 are routed to consumer2
        final List<Entry> allEntries = new ArrayList<>();
        allEntries.add(EntryImpl.create(1, 1, createMessage("message1", 1, "key2")));
        allEntries.add(EntryImpl.create(1, 2, createMessage("message2", 2, "key1")));
        allEntries.add(EntryImpl.create(1, 3, createMessage("message3", 3, "key1")));
        allEntries.forEach(entry -> ((EntryImpl) entry).retain());

        final List<Entry> redeliverEntries = new ArrayList<>();
        redeliverEntries.add(allEntries.get(0)); // message1
        final List<Entry> readEntries = new ArrayList<>();
        readEntries.add(allEntries.get(2)); // message3

        final Consumer consumer1 = mock(Consumer.class);
        doReturn("consumer1").when(consumer1).consumerName();
        // Change availablePermits of consumer1 to 0 and then back to normal
        when(consumer1.getAvailablePermits()).thenReturn(0).thenReturn(10);
        doReturn(true).when(consumer1).isWritable();
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            List<Entry> entries = (List<Entry>) invocationOnMock.getArgument(0);
            for (Entry entry : entries) {
                remainingEntriesNum.decrementAndGet();
                actualEntriesToConsumer1.add(entry.getPosition());
            }
            return channelMock;
        }).when(consumer1).sendMessages(anyList(), any(EntryBatchSizes.class), any(EntryBatchIndexesAcks.class),
                anyInt(), anyLong(), anyLong(), any(RedeliveryTracker.class));

        final Consumer consumer2 = mock(Consumer.class);
        doReturn("consumer2").when(consumer2).consumerName();
        when(consumer2.getAvailablePermits()).thenReturn(10);
        doReturn(true).when(consumer2).isWritable();
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            List<Entry> entries = (List<Entry>) invocationOnMock.getArgument(0);
            for (Entry entry : entries) {
                remainingEntriesNum.decrementAndGet();
                actualEntriesToConsumer2.add(entry.getPosition());
            }
            return channelMock;
        }).when(consumer2).sendMessages(anyList(), any(EntryBatchSizes.class), any(EntryBatchIndexesAcks.class),
                anyInt(), anyLong(), anyLong(), any(RedeliveryTracker.class));

        persistentDispatcher.addConsumer(consumer1);
        persistentDispatcher.addConsumer(consumer2);

        final Field totalAvailablePermitsField = PersistentDispatcherMultipleConsumersClassic.class
                .getDeclaredField("totalAvailablePermits");
        totalAvailablePermitsField.setAccessible(true);
        totalAvailablePermitsField.set(persistentDispatcher, 1000);

        final Field redeliveryMessagesField = PersistentDispatcherMultipleConsumersClassic.class
                .getDeclaredField("redeliveryMessages");
        redeliveryMessagesField.setAccessible(true);
        MessageRedeliveryController redeliveryMessages = (MessageRedeliveryController) redeliveryMessagesField
                .get(persistentDispatcher);
        redeliveryMessages.add(allEntries.get(0).getLedgerId(), allEntries.get(0).getEntryId(),
                persistentDispatcher.getStickyKeyHash(allEntries.get(0))); // message1
        redeliveryMessages.add(allEntries.get(1).getLedgerId(), allEntries.get(1).getEntryId(),
                persistentDispatcher.getStickyKeyHash(allEntries.get(1))); // message2

        // Mock Cursor#asyncReplayEntries
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Set<Position> positions = (Set<Position>) invocationOnMock.getArgument(0);
            List<Entry> entries = allEntries.stream().filter(entry -> positions.contains(entry.getPosition()))
                    .collect(Collectors.toList());
            if (!entries.isEmpty()) {
                ((PersistentStickyKeyDispatcherMultipleConsumersClassic) invocationOnMock.getArgument(1))
                        .readEntriesComplete(entries, PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Replay);
            }
            return Collections.emptySet();
        }).when(cursorMock).asyncReplayEntries(anySet(), any(PersistentStickyKeyDispatcherMultipleConsumersClassic.class),
                eq(PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Replay), anyBoolean());

        // Mock Cursor#asyncReadEntriesOrWait
        AtomicBoolean asyncReadEntriesOrWaitCalled = new AtomicBoolean();
        doAnswer(invocationOnMock -> {
            if (asyncReadEntriesOrWaitCalled.compareAndSet(false, true)) {
                ((PersistentStickyKeyDispatcherMultipleConsumersClassic) invocationOnMock.getArgument(2))
                        .readEntriesComplete(readEntries, PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Normal);
            } else {
                ((PersistentStickyKeyDispatcherMultipleConsumersClassic) invocationOnMock.getArgument(2))
                        .readEntriesComplete(Collections.emptyList(), PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Normal);
            }
            return null;
        }).when(cursorMock).asyncReadEntriesOrWait(anyInt(), anyLong(),
                any(PersistentStickyKeyDispatcherMultipleConsumersClassic.class),
                eq(PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Normal), any());

        // (1) Run sendMessagesToConsumers
        // (2) Attempts to send message1 to consumer1 but skipped because availablePermits is 0
        // (3) Change availablePermits of consumer1 to 10
        // (4) Run readMoreEntries internally
        // (5) Run sendMessagesToConsumers internally
        // (6) Attempts to send message3 to consumer2 but skipped because redeliveryMessages contains message2
        persistentDispatcher.sendMessagesToConsumers(PersistentStickyKeyDispatcherMultipleConsumersClassic.ReadType.Replay,
                redeliverEntries, true);
        while (remainingEntriesNum.get() > 0) {
            // (7) Run readMoreEntries and resend message1 to consumer1 and message2-3 to consumer2
            persistentDispatcher.readMoreEntries();
        }

        assertThat(actualEntriesToConsumer1).containsExactlyElementsOf(expectedEntriesToConsumer1);
        assertThat(actualEntriesToConsumer2).containsExactlyElementsOf(expectedEntriesToConsumer2);

        allEntries.forEach(entry -> entry.release());
    }

    private ByteBuf createMessage(String message, int sequenceId) {
        return createMessage(message, sequenceId, "testKey");
    }

    private ByteBuf createMessage(String message, int sequenceId, String key) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKey(key)
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis());
        return serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, Unpooled.copiedBuffer(message.getBytes(UTF_8)));
    }
}
