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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.io.udp.transport.UDPNettyServer;
import io.siddhi.extension.io.udp.transport.config.UDPServerConfig;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple tests for the UDP Source extension with the text mapper.
 * This test case sends mock UDP packet with a text payload to the UDP source Siddhi extension.
 */
public class SimpleUDPSourceTestCase {
        // If you will know about this related testcase,
        //refer https://github.com/siddhi-io/siddhi-io-file/blob/master/component/src/test

    private static final Logger log = Logger.getLogger(UDPNettyServer.class);
    private SiddhiAppRuntime siddhiAppRuntime;
    private List<Event[]> events;

    @BeforeMethod
    public void setUp() {
        log.info("In setUp()");
        events = new ArrayList<>();

        // Starting Siddhi Runtime
        final SiddhiManager siddhiManager = new SiddhiManager();

        final String inStreamDefinition =
                "@app:name('Simple-UDP-Tests')" +
                "@source(type='udp', listen.port='5556', @map(type='text'))\n" +
                "define stream inputStream (foo long);";
        final String query =
                "@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;";
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
    public void tearDown() {
        log.info("In tearDown()");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testSendTextPayload() throws Exception {
        final int numTestEvents = 100;
        sendTestEvents("foo:123".getBytes(StandardCharsets.UTF_8), numTestEvents);

        // Wait a sec for the processing to complete
        Thread.sleep(500);
        Assert.assertEquals(events.size(), numTestEvents);
        for (final Event[] eventArr : events) {
            Assert.assertEquals(eventArr.length, 1);
            final Event event = eventArr[0];
            Assert.assertEquals(event.getData().length, 1);
            Assert.assertEquals(event.getData()[0], 123L);
        }
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
        }
    }

}
