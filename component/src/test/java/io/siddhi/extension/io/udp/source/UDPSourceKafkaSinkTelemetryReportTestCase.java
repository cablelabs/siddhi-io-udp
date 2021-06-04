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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.io.udp.TestTelemetryReports;
import io.siddhi.extension.io.udp.transport.UDPNettyServer;
import io.siddhi.extension.io.udp.transport.config.UDPServerConfig;
import io.siddhi.extension.map.p4.trpt.TelemetryReport;
import io.siddhi.extension.map.p4.trpt.TelemetryReportHeader;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


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
    private SharedKafkaTestResource kafka;

    @BeforeMethod
    public void setUp() throws Exception {
        log.info("In setUp()");
        events = new ArrayList<>();
        kafka = new SharedKafkaTestResource();

        final String inStreamDefinition =
                "@app:name('Filter-TRPT-Tests')" +
                "@source(type='udp', listen.port='5556', @map(type='p4-trpt'))" +
                // TODO - When sink is defined, only one event is sent
//                "@sink(type='kafka', topic='test', bootstrap.servers='localhost:9092', @map(type='text'))" +
                "define stream trptUdpPktStream (trptJson String);";
        final String query =
                "@info(name = 'query1') " +
                "from trptUdpPktStream " +
                "select *  " +
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

    }

    @AfterMethod
    public void tearDown() throws Exception {
        log.info("In tearDown()");
//        for (final KafkaBroker broker : KAFKA.getKafkaBrokers().asList()) {
//            broker.stop();
//        }
        siddhiAppRuntime.shutdown();
//        log.info("Siddhi App Runtime down");
    }

//    private void createSiddhiRuntime(final String queryName, final String siddhiStr) {
//        final SiddhiManager siddhiManager = new SiddhiManager();
//        siddhiManager.setExtension("kafka", KafkaSink.class);
//        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiStr);
//        siddhiAppRuntime.addCallback(queryName, new QueryCallback() {
//            @Override
//            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
//                events.add(inEvents);
//            }
//        });
//        siddhiAppRuntime.start();
//    }

    @Test
    public void testTelemetryReportUdp4() throws Exception {

        final int numTestEvents = 10;
        sendTestEvents(TestTelemetryReports.UDP4_2HOPS, numTestEvents);

        // Wait a sec for the processing to complete
        Thread.sleep(10000);
        Assert.assertEquals(numTestEvents, events.size());

        // TODO - Add topic listener and validate payload received
        validateTelemetryReports();
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
            Thread.sleep(100);
        }
    }

    private void validateTelemetryReports() {
        for (final Event[] eventArr : events) {
            Assert.assertEquals(1, eventArr.length);
            final Object[] eventObjs = eventArr[0].getData();
            Assert.assertEquals(1, eventObjs.length);
            Assert.assertTrue(eventObjs[0] instanceof String);

            final JsonParser jsonParser = new JsonParser();
            final JsonObject jsonObj = jsonParser.parse((String) eventObjs[0]).getAsJsonObject();
            Assert.assertNotNull(jsonObj);
            final JsonObject trptHdrJson = jsonObj.getAsJsonObject(TelemetryReport.TRPT_HDR_KEY);
            org.junit.Assert.assertNotNull(trptHdrJson);
            Assert.assertEquals(2, trptHdrJson.get(TelemetryReportHeader.TRPT_VER_KEY).getAsInt());
            Assert.assertEquals(234, trptHdrJson.get(TelemetryReportHeader.TRPT_NODE_ID_KEY).getAsLong());
            Assert.assertEquals(21587, trptHdrJson.get(TelemetryReportHeader.TRPT_DOMAIN_ID_KEY).getAsLong());
        }
    }

}

