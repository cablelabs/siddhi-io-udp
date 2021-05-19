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
