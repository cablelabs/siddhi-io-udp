package io.siddhi.extension.io.udp.transport.config;

/**
 * Overrides the io.siddhi.tcp server configuration as this module is using some of it's classes.
 */
public class UDPServerConfig extends io.siddhi.extension.io.tcp.transport.config.ServerConfig {

    static final int DEFAULT_UDP_PORT = 5556;

    public UDPServerConfig() {
        super();
        this.setPort(DEFAULT_UDP_PORT);
    }
}
