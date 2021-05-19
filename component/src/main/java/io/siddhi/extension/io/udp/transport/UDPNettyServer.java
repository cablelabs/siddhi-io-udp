package io.siddhi.extension.io.udp.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.siddhi.extension.io.udp.transport.config.UDPServerConfig;
import org.apache.log4j.Logger;

/**
 * Starts a server listening for UDP packets.
 */
public final class UDPNettyServer {

    private static final Logger log = Logger.getLogger(UDPNettyServer.class);
    private final Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;
    private final UDPServerConfig serverConfig;
    private final String hostAndPort;
    private ChannelFuture channelFuture;

    /**
     * Constructor.
     * @param serverConf - the server's configuration
     */
    public UDPNettyServer(final UDPServerConfig serverConf) {
        bootstrap = new Bootstrap();
        serverConfig = serverConf;
        hostAndPort = serverConfig.getHost() + ":" + serverConfig.getPort();
    }

    /**
     * Starts the server.
     */
    public void start(final ChannelHandler handler) {
        eventLoopGroup = new NioEventLoopGroup(serverConfig.getReceiverThreads());
        bootstrap.group(eventLoopGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(handler);
        try {
            // Bind and start to accept incoming connections.
            channelFuture = bootstrap.bind(serverConfig.getPort()).sync().await();
            log.info("UDP Server started at host:port - " + hostAndPort + "");
        } catch (InterruptedException e) {
            log.error("Error when booting up UDP server on '" + hostAndPort + "' " + e.getMessage(), e);
        }
    }

    public void shutdownGracefully() {
        if (channelFuture != null) {
            channelFuture.channel().closeFuture();
            channelFuture.channel().close();
            try {
                channelFuture.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                log.error("Error when shutting down the UDP server " + e.getMessage(), e);
            }
        }

        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
            eventLoopGroup = null;
        }
        log.info("UDP Server running on '" + hostAndPort + "' stopped.");
    }
}

