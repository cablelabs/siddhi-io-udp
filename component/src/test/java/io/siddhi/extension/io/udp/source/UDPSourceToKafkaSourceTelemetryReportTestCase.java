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
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;


/**
 * Tests for the UDP Source extension with the p4-trpt mapper to ensure that the events get sent out to Kafka with
 * "json" mapping and received by the kafka source with "json" mapping.
 */
public class UDPSourceToKafkaSourceTelemetryReportTestCase {
    // If you will know about this related testcase,
    //refer https://github.com/siddhi-io/siddhi-io-file/blob/master/component/src/test

    private static final Logger log = Logger.getLogger(UDPNettyServer.class);
    private SiddhiAppRuntime srcUdpSiddhiAppRuntime;
    private SiddhiAppRuntime kafkaIngressSiddhiAppRuntime;
    private List<Event[]> parsedUdpEvents;
    private List<Event[]> kafkaIngressEvents;
    private String testTopic;
    private final String kafkaServer = "wso2-vm:9092";
    private static final int numTestEvents = 1000;
    private static final int waitMs = 100;


    @BeforeMethod
    public void setUp() {
        log.info("In setUp()");
        parsedUdpEvents = new ArrayList<>();
        kafkaIngressEvents = new ArrayList<>();
        testTopic = UUID.randomUUID().toString();

        final SiddhiManager siddhiManager = new SiddhiManager();
        createSrcUdpAppRuntime(siddhiManager);
        createKafkaIngressAppRuntime(siddhiManager);
    }

    @AfterMethod
    public void tearDown() {
        log.info("In tearDown()");
        try {
            srcUdpSiddhiAppRuntime.shutdown();
            kafkaIngressSiddhiAppRuntime.shutdown();
        } finally {
            // Delete topic
            final Properties conf = new Properties();
            conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
            conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
            final AdminClient client = AdminClient.create(conf);
            log.info("Deleting testTopic - " + testTopic);
            client.deleteTopics(Collections.singletonList(testTopic));
            client.close();
        }

        log.info("Siddhi App Runtime down");
    }

    /**
     * Tests to ensure that UDP IPv4 two hop Telemetry Report packets can be converted to JSON, sent out and received
     * back via Kafka.
     */
    @Test
    public void testTelemetryReportUdp4() {
        runTest(TestTelemetryReports.UDP4_2HOPS);
    }

    /**
     * Tests to ensure that TCP IPv4 two hop Telemetry Report packets can be converted to JSON, sent out and received
     * back via Kafka.
     */
    @Test
    public void testTelemetryReportTcp4() {
        runTest(TestTelemetryReports.TCP4_2HOPS);
    }

    /**
     * Tests to ensure that UDP IPv6 two hop Telemetry Report packets can be converted to JSON, sent out and received
     * back via Kafka.
     */
    @Test
    public void testTelemetryReportUdp6() {
        runTest(TestTelemetryReports.UDP6_2HOPS);
    }

    /**
     * Tests to ensure that TCP IPv4 two hop Telemetry Report packets can be converted to JSON, sent out and received
     * back via Kafka.
     */
    @Test
    public void testTelemetryReportTcp6() {
        runTest(TestTelemetryReports.TCP6_2HOPS);
    }

    /**
     * Executes the test against the byte array.
     * @param bytes - the packet UDP payload (the TelemetryReport bytes)
     * @throws Exception
     */
    private void runTest(final byte[] bytes) {
        try {
            sendTestEvents(bytes, numTestEvents);

            // Wait a short bit for the processing to complete
            Thread.sleep(waitMs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(numTestEvents, kafkaIngressEvents.size());
        Assert.assertEquals(numTestEvents, parsedUdpEvents.size());

        validateTelemetryReports();
    }

    /**
     * Method responsible for starting the kafka source Siddhi script with "json" mapper.
     * TODO - Properly define trptJsonStream and write one DDoS query
     * TODO - Add HTTP sink for making web service call to mock SDN controller
     * @param siddhiManager - the Siddhi manager to leverage
     */
    private void createKafkaIngressAppRuntime(final SiddhiManager siddhiManager) {
        final String udpSourceDefinition = String.format(
                "@app:name('Kafka-Source-TRPT')\n" +
                    "@source(type='kafka', topic.list='%s', bootstrap.servers='%s', group.id='test', " +
                    "threading.option='single.thread', @map(type='json'))\n" +
                    "define stream trptJsonStream (trptJson OBJECT);\n", testTopic, kafkaServer);
        final String udpSourceQuery =
                "@info(name = 'sourceQuery')\n" +
                    "from trptJsonStream\n" +
                    "select *\n" +
                    "insert into parsedTrpt;";
        kafkaIngressSiddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(udpSourceDefinition + udpSourceQuery);
        kafkaIngressSiddhiAppRuntime.addCallback("sourceQuery", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                kafkaIngressEvents.add(inEvents);
            }
        });
        kafkaIngressSiddhiAppRuntime.start();
    }

    /**
     * Method responsible for starting the UDP source siddhi script with the "p4-trpt" mapper and kafka sink with the
     * "json" mapper.
     * @param siddhiManager - the Siddhi manager to leverage
     */
    private void createSrcUdpAppRuntime(final SiddhiManager siddhiManager) {
        final String udpSourceDefinition = String.format(
                "@app:name('UDP-Source-TRPT')\n" +
                    "@source(type='udp', listen.port='5556', @map(type='p4-trpt'))\n" +
                    "@sink(type='kafka', topic='%s', bootstrap.servers='%s', @map(type='json'))\n" +
                    "define stream trptUdpPktStream (trptJson OBJECT);\n", testTopic, kafkaServer);
        final String udpSourceQuery =
                "@info(name = 'sourceQuery')\n" +
                    "from trptUdpPktStream\n" +
                    "select *\n" +
                    "insert into distTrptJson;";
        srcUdpSiddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(udpSourceDefinition + udpSourceQuery);
        srcUdpSiddhiAppRuntime.addCallback("sourceQuery", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                parsedUdpEvents.add(inEvents);
            }
        });
        srcUdpSiddhiAppRuntime.start();
    }

    private void sendTestEvents(final byte[] eventBytes, final int numTestEvents) throws Exception {
        for (int ctr = 0; ctr < numTestEvents; ctr++) {
            final InetAddress address = InetAddress.getByName("localhost");
            final DatagramPacket packet = new DatagramPacket(eventBytes, 0, eventBytes.length,
                    address, new UDPServerConfig().getPort());
            final DatagramSocket datagramSocket = new DatagramSocket();
            datagramSocket.send(packet);
            Thread.sleep(1, 100000);
        }
    }

    private void validateTelemetryReports() {
        for (final Event[] events : parsedUdpEvents) {
            for (final Event event : events) {
                validateEvent(event);
            }
        }
        for (final Event[] events : kafkaIngressEvents) {
            for (final Event event : events) {
                validateEvent(event);
            }
        }
    }

    private void validateEvent(final Event event) {
        final String eventStr = (String) event.getData()[0];
        final JsonParser parser = new JsonParser();
        final JsonElement jsonElement = parser.parse(eventStr);
        final JsonObject trptJsonObj = jsonElement.getAsJsonObject();
        Assert.assertNotNull(trptJsonObj);
        final JsonObject trptHdrJson = trptJsonObj.get("telemRptHdr").getAsJsonObject();
        Assert.assertNotNull(trptHdrJson);
        Assert.assertEquals(2, trptHdrJson.get(TelemetryReportHeader.TRPT_VER_KEY).getAsInt());
        Assert.assertEquals(234, trptHdrJson.get(TelemetryReportHeader.TRPT_NODE_ID_KEY).getAsLong());
        Assert.assertEquals(21587, trptHdrJson.get(TelemetryReportHeader.TRPT_DOMAIN_ID_KEY).getAsLong());
    }

}
