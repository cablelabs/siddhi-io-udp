/*
 * Copyright (c) 2021 Cable Television Laboratories, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.siddhi.extension.io.udp.source;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.io.udp.TestTelemetryReports;
import io.siddhi.extension.io.udp.transport.UDPNettyServer;
import io.siddhi.extension.io.udp.transport.config.UDPServerConfig;
import io.siddhi.extension.map.p4.trpt.TelemetryReportHeader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;


/**
 * Simple tests for the UDP Source extension with the p4-trpt mapper.
 * This test case sends mock Telemetry report UDP packets to this UDP source Siddhi extension and ensures each of the
 * resulting JSON documents contains the expected values.
 */
public class UDPSourceKafkaSinkTelemetryReportTestCase {
    // If you will know about this related testcase,
    //refer https://github.com/siddhi-io/siddhi-io-file/blob/master/component/src/test

    private static final Logger log = Logger.getLogger(UDPNettyServer.class);
    private SiddhiAppRuntime siddhiAppRuntime;
    private List<Event[]> events;
    private final String kafkaServer = "wso2-vm:9092";
    private String testTopic;
//    private SharedKafkaTestResource kafka;
    private KafkaRunnable consumerRunnable;

    @BeforeMethod
    public void setUp() throws Exception {
        log.info("In setUp()");
        events = new ArrayList<>();
//        kafka = new SharedKafkaTestResource();
        testTopic = UUID.randomUUID().toString();

        final String inStreamDefinition = String.format(
                "@app:name('Filter-TRPT-Tests')\n" +
                "@source(type='udp', listen.port='5556', @map(type='p4-trpt'))\n" +
                "@sink(type='kafka', topic='%s', bootstrap.servers='%s'," +
                    "@map(type='text'))\n" +
                "define stream trptUdpPktStream (trptJson OBJECT);\n", testTopic, kafkaServer);
        final String query =
                "@info(name = 'query1')\n" +
                "from trptUdpPktStream\n" +
                "select *\n" +
                "insert into outputStream;";
        final SiddhiManager siddhiManager = new SiddhiManager();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                events.add(inEvents);
            }
        });
        siddhiAppRuntime.start();

        consumerRunnable = runConsumer();

    }

    @AfterMethod
    public void tearDown() throws Exception {
        log.info("In tearDown()");
        siddhiAppRuntime.shutdown();
        final Properties conf = new Properties();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "wso2-vm:9092");
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        final AdminClient client = AdminClient.create(conf);
        client.deleteTopics(Collections.singletonList(testTopic));

//        for (final KafkaBroker broker : KAFKA.getKafkaBrokers().asList()) {
//            broker.stop();
//        }

        // Delete topic
        log.info("Siddhi App Runtime down");
    }

    @Test
    public void testTelemetryReportUdp4() throws Exception {
        final int numTestEvents = 50;
        sendTestEvents(TestTelemetryReports.UDP4_2HOPS, numTestEvents);

        // Wait a sec for the processing to complete
        Thread.sleep(2000);

        validateTelemetryReports();

        Assert.assertEquals(consumerRunnable.events.size(), consumerRunnable.numRecordsCount);
        Assert.assertEquals(numTestEvents, consumerRunnable.numRecordsCount);
        Assert.assertEquals(numTestEvents, consumerRunnable.events.size());
    }

    private KafkaRunnable runConsumer() {
        final Consumer<Long, String> consumer = createConsumer();
        final KafkaRunnable out = new KafkaRunnable(consumer);
        final Thread kafkaConsumerThread = new Thread(out);
        kafkaConsumerThread.start();
        return out;
    }

    private Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "UDPSourceKafkaSinkTelemetryReportTestCase-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(testTopic));
        return consumer;
    }

    private void sendTestEvents(final byte[] eventBytes, final int numTestEvents) throws Exception {
        for (int ctr = 0; ctr < numTestEvents; ctr++) {
            final InetAddress address = InetAddress.getByName("localhost");
            final DatagramPacket packet = new DatagramPacket(eventBytes, 0, eventBytes.length,
                    address, new UDPServerConfig().getPort());
            // TODO - need to get this
            // configReader.readConfig(KEEP_ALIVE, "" + Constant.DEFAULT_KEEP_ALIVE)
            final DatagramSocket datagramSocket = new DatagramSocket();
            datagramSocket.send(packet);
            Thread.sleep(1);
        }
    }

    private void validateTelemetryReports() {
        for (final String eventStr : consumerRunnable.events) {
            final JsonParser parser = new JsonParser();
            final JsonElement jsonElement = parser.parse(eventStr.replaceAll("trptJson:", ""));
            final JsonObject trptJsonObj = jsonElement.getAsJsonObject();
            Assert.assertNotNull(trptJsonObj);
            final JsonObject trptHdrJson = trptJsonObj.get("telemRptHdr").getAsJsonObject();
            Assert.assertNotNull(trptHdrJson);
            Assert.assertEquals(2, trptHdrJson.get(TelemetryReportHeader.TRPT_VER_KEY).getAsInt());
            Assert.assertEquals(234, trptHdrJson.get(TelemetryReportHeader.TRPT_NODE_ID_KEY).getAsLong());
            Assert.assertEquals(21587, trptHdrJson.get(TelemetryReportHeader.TRPT_DOMAIN_ID_KEY).getAsLong());
        }
    }

    private class KafkaRunnable implements Runnable {

        final Consumer<Long, String> consumer;
        int numRecordsCount = 0;
        final List<String> events = new ArrayList<>();

        KafkaRunnable(final Consumer<Long, String> consumer) {
            this.consumer = consumer;
        }

        private boolean running = true;

        public synchronized void stop() {
            running = false;
        }

        @Override
        public synchronized void run() {
            while (running) {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                if (consumerRecords.count() != 0) {
                    numRecordsCount += consumerRecords.count();

                    consumerRecords.forEach(record -> {
                        System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                                record.key(), record.value(),
                                record.partition(), record.offset());
                        events.add(record.value());
                    });
                }

                consumer.commitSync();
            }
            consumer.close();
        }
    }
}

