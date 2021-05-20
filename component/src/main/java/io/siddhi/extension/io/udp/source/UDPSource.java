package io.siddhi.extension.io.udp.source;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.tcp.transport.utils.Constant;
import io.siddhi.extension.io.udp.transport.IncomingPacketHandler;
import io.siddhi.extension.io.udp.transport.UDPNettyServer;
import io.siddhi.extension.io.udp.transport.config.UDPServerConfig;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Siddhi UDP source extension.
 * for more information refer https://siddhi.io/en/v5.0/docs/query-guide/#source
 */
@Extension(
        name = "udp",
        namespace = "source",
        description = "UDP Source for a Siddhi application. Listens to a port and sends the payload as a byte array",
        systemParameter = {
                @SystemParameter(
                        name = "host",
                        description = "Tcp server host.",
                        defaultValue = "0.0.0.0",
                        possibleParameters = "Any valid host or IP"
                ),
                @SystemParameter(
                        name = "port",
                        description = "Tcp server port.",
                        defaultValue = "5556",
                        possibleParameters = "Any integer representing valid port"
                ),
                @SystemParameter(
                        name = "receiver.threads",
                        description = "Number of threads to receive connections.",
                        defaultValue = "10",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "worker.threads",
                        description = "Number of threads to serve events.",
                        defaultValue = "10",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "keep.alive",
                        description = "This property defines whether the server should be kept alive when " +
                                "there are no connections available.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"}
                )
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@Source(type = 'udp', port=556, @map(type='p4-trpt'))\n" +
                                "define stream inputStream (a object);",
                        description = "" +
                                "Under this configuration, events are received via the UDP transport on default host," +
                                "port, and mapped in p4-trpt before they are passed to `inputStream` stream for " +
                                "additional processing."

                )
        }
)

/**
 * The Sidhhi Source class for receiving UDP packets
 */
public class UDPSource extends Source {

    private static final String RECEIVER_THREADS = "receiver.threads";
    private static final String WORKER_THREADS = "worker.threads";
    private static final String PORT = "port";
    private static final String HOST = "host";
    private static final String TCP_NO_DELAY = "tcp.no.delay";
    private static final String KEEP_ALIVE = "keep.alive";
    private static final int DEFAULT_PORT = 5556;

    private SourceEventListener sourceEventListener;
    private UDPNettyServer server;
    private UDPServerConfig serverConfig;

    /**
     * The initialization method for {@link Source}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     *
     * @param sourceEventListener             The listener to pass the events for processing which are consumed
     *                                        by the source
     * @param optionHolder                    Contains static options of the source
     * @param requestedTransportPropertyNames Requested transport properties that should be passed to
     *                                        SourceEventListener
     * @param configReader                    System configuration reader for source
     * @param siddhiAppContext                Siddhi application context
     * @return StateFactory for the Function which contains logic for the updated state based on arrived events.
     */
    @Override
    public StateFactory init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
        String[] requestedTransportPropertyNames, ConfigReader configReader,
        SiddhiAppContext siddhiAppContext) {
        if (requestedTransportPropertyNames != null && requestedTransportPropertyNames.length > 0) {
            throw new SiddhiAppCreationException("'tcp' source does not support requestedTransportProperties," +
                    " but at stream '" + getStreamDefinition().getId() + "' '" +
                    Arrays.deepToString(requestedTransportPropertyNames) +
                    "' transport properties are requested");
        }

        this.sourceEventListener = sourceEventListener;
        serverConfig = new UDPServerConfig();
        serverConfig.setHost(configReader.readConfig(HOST, Constant.DEFAULT_HOST));
        serverConfig.setPort(Integer.parseInt(configReader.readConfig(PORT, "" + DEFAULT_PORT)));
        serverConfig.setKeepAlive(Boolean.parseBoolean((configReader.readConfig(
                KEEP_ALIVE, "" + Constant.DEFAULT_KEEP_ALIVE))));
        serverConfig.setTcpNoDelay(Boolean.parseBoolean((configReader.readConfig(
                TCP_NO_DELAY, "" + Constant.DEFAULT_TCP_NO_DELAY))));
        serverConfig.setReceiverThreads(Integer.parseInt((configReader.readConfig(
                RECEIVER_THREADS, "" + Constant.DEFAULT_RECEIVER_THREADS))));
        serverConfig.setWorkerThreads(
                Integer.parseInt((configReader.readConfig(
                        WORKER_THREADS, "" + Constant.DEFAULT_WORKER_THREADS))));
        return null;
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, byte[].class, ByteBuffer.class};
//        return new Class[]{byte[].class};
    }

    /**
     * Give information to the deployment about the service exposed by the sink.
     *
     * @return ServiceDeploymentInfo  Service related information to the deployment
     */
    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    /**
     * Initially Called to connect to the end point for start retrieving the messages asynchronously.
     *
     * @param connectionCallback Callback to pass the ConnectionUnavailableException in case of connection failure after
     *                           initial successful connection. (can be used when events are receiving asynchronously)
     * @param state              current state of the source
     */
    @Override
    public void connect(ConnectionCallback connectionCallback, State state) {
        server = new UDPNettyServer(serverConfig);
        server.start(new IncomingPacketHandler(sourceEventListener));
    }

    /**
     * Called to pause event consumption.
     */
    @Override
    public void pause() {

    }

    /**
     * Called to resume event consumption.
     */
    @Override
    public void resume() {

    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {
//        this.destroy();
    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}.
     */
    @Override
    public void destroy() {
        if (server != null) {
            server.shutdownGracefully();
        }
    }

}
