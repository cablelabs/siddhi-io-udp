package io.siddhi.extension.io.udp.source;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.io.udp.TestTelemetryReports;
import io.siddhi.extension.io.udp.transport.UDPNettyServer;
import io.siddhi.extension.io.udp.transport.config.UDPServerConfig;
import io.siddhi.extension.map.p4.trpt.TelemetryReport;
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
 * Testcase of UDPSource.
 */
public class UDPSourceTelemetryReportTestCase {
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
//        siddhiManager.setExtension("p4-trpt", P4TrptSourceMapper.class);

        final String inStreamDefinition = "" +
                "@app:name('udpTest')" +
                "@source(type='udp', @map(type='p4-trpt'))" + // This requires
                "define stream inputStream (a object);";
        final String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
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
    public void testTelemetryReportUdp4() throws Exception {
        final int numTestEvents = 10;
        sendTestEvents(TestTelemetryReports.UDP4_2HOPS, numTestEvents);

        // Wait a sec for the processing to complete
        Thread.sleep(1000);
        Assert.assertEquals(numTestEvents, events.size());
        validateTelemetryReports();
    }

    @Test
    public void testTelemetryReportUdp6() throws Exception {
        final int numTestEvents = 10;
        sendTestEvents(TestTelemetryReports.UDP6_2HOPS, numTestEvents);

        // Wait a sec for the processing to complete
        Thread.sleep(1000);
        Assert.assertEquals(numTestEvents, events.size());
        validateTelemetryReports();
    }

    @Test
    public void testTelemetryReportTcp4() throws Exception {
        final int numTestEvents = 10;
        sendTestEvents(TestTelemetryReports.TCP4_2HOPS, numTestEvents);

        // Wait a sec for the processing to complete
        Thread.sleep(1000);
        Assert.assertEquals(numTestEvents, events.size());
        validateTelemetryReports();
    }

    @Test
    public void testTelemetryReportTcp6() throws Exception {
        final int numTestEvents = 10;
        sendTestEvents(TestTelemetryReports.TCP6_2HOPS, numTestEvents);

        // Wait a sec for the processing to complete
        Thread.sleep(1000);
        Assert.assertEquals(numTestEvents, events.size());
        validateTelemetryReports();
    }

    private void sendTestEvents(final byte[] eventBytes, final int numTestEvents) throws Exception {
        for (int ctr = 0; ctr < numTestEvents; ctr++) {
            final InetAddress address = InetAddress.getByName("localhost");
            final DatagramPacket packet = new DatagramPacket(eventBytes, 0, eventBytes.length,
                    address, new UDPServerConfig().getPort());
            final DatagramSocket datagramSocket = new DatagramSocket();
            datagramSocket.send(packet);
        }
    }

    private void validateTelemetryReports() {
        for (final Event[] eventArr : events) {
            Assert.assertEquals(1, eventArr.length);
            final Object[] eventObjs = eventArr[0].getData();
            Assert.assertEquals(1, eventObjs.length);
            Assert.assertTrue(eventObjs[0] instanceof TelemetryReport);
            final TelemetryReport trpt = (TelemetryReport) eventObjs[0];
            Assert.assertEquals(2, trpt.trptHdr.getVersion());
            Assert.assertEquals(234, trpt.trptHdr.getNodeId());
            Assert.assertEquals(21587, trpt.trptHdr.getDomainId());
        }
    }

}
