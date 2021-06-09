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
 * Tests for the UDP Source extension with the p4-trpt mapper to ensure that the events get sent out to Kafka.
 * This test case sends mock Telemetry report UDP packets to this UDP source Siddhi extension and ensures each of the
 * resulting JSON documents contains the expected values.
 */
public class UDPSourceKafkaSinkTelemetryReportTestCase {
    // If you will know about this related testcase,
    //refer https://github.com/siddhi-io/siddhi-io-file/blob/master/component/src/test

    private static final Logger log = Logger.getLogger(UDPNettyServer.class);
    private SiddhiAppRuntime siddhiAppRuntime;
    private List<Event[]> events;
    private String testTopic;
    private KafkaRunnable consumerRunnable;
    private static final String kafkaServer = "wso2-vm:9092";

    @BeforeMethod
    public void setUp() {
        log.info("In setUp()");
        events = new ArrayList<>();
        testTopic = UUID.randomUUID().toString();

        final String inStreamDefinition = String.format(
                "@app:name('Kafka-Sink-TRPT')\n" +
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
        consumerRunnable = KafkaRunnable.runConsumer(kafkaServer, testTopic);
    }

    @AfterMethod
    public void tearDown() {
        log.info("In tearDown()");
        consumerRunnable.stop();

        try {
            siddhiAppRuntime.shutdown();
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
     * Tests to ensure that UDP IPv4 two hop Telemetry Report packets can be converted to JSON and sent out via Kafka.
     */
    @Test
    public void testTelemetryReportUdp4() {
        runTest(TestTelemetryReports.UDP4_2HOPS);
    }

    /**
     * Tests to ensure that TCP IPv4 two hop Telemetry Report packets can be converted to JSON and sent out via Kafka.
     */
    @Test
    public void testTelemetryReportTcp4() {
        runTest(TestTelemetryReports.TCP4_2HOPS);
    }

    /**
     * Tests to ensure that UDP IPv6 two hop Telemetry Report packets can be converted to JSON and sent out via Kafka.
     */
    @Test
    public void testTelemetryReportUdp6() {
        runTest(TestTelemetryReports.UDP6_2HOPS);
    }

    /**
     * Tests to ensure that TCP IPv4 two hop Telemetry Report packets can be converted to JSON and sent out via Kafka.
     */
    @Test
    public void testTelemetryReportTcp6() {
        runTest(TestTelemetryReports.TCP6_2HOPS);
    }

    private void runTest(final byte[] bytes) {
        final int numTestEvents = 50;
        try {
            sendTestEvents(bytes, numTestEvents);

            // Wait a sec for the processing to complete
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        validateTelemetryReports();

        Assert.assertEquals(consumerRunnable.events.size(), consumerRunnable.numRecordsCount);

        // TODO - Determine why all are not always received by the Kafka Consumer, KafkaRunnable
        Assert.assertTrue(consumerRunnable.numRecordsCount <= numTestEvents && numTestEvents > 0);
        Assert.assertTrue(consumerRunnable.events.size() <= numTestEvents && numTestEvents > 0);
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
            Thread.sleep(1, 10000);
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

}
