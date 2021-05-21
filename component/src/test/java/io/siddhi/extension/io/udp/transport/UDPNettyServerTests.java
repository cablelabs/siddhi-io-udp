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

package io.siddhi.extension.io.udp.transport;

import io.siddhi.extension.io.udp.TestTelemetryReports;
import io.siddhi.extension.io.udp.transport.config.UDPServerConfig;
import org.junit.Assert;
import org.junit.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Unit tests for the class UDPNettyServer.
 */
public class UDPNettyServerTests {

    @Test
    public void testReceivePackets() throws Exception {
        final UDPServerConfig serverConf = new UDPServerConfig();
        final UDPNettyServer server = new UDPNettyServer(serverConf);
        final IncomingPacketHandler packetHandler = new IncomingPacketHandler(null);
        server.start(packetHandler);

        int ctr = 0;
        while (ctr < 10) {
            final InetAddress address = InetAddress.getByName("localhost");
            final DatagramPacket packet = new DatagramPacket(
                    TestTelemetryReports.UDP4_2HOPS, TestTelemetryReports.UDP4_2HOPS.length);
            packet.setAddress(address);
            packet.setPort(serverConf.getPort());
            final DatagramSocket datagramSocket = new DatagramSocket();
            datagramSocket.send(packet);
            Thread.sleep(100);
            ctr++;
        }
        Assert.assertEquals(10, packetHandler.getPacketCount());
        server.shutdownGracefully();
    }
}
