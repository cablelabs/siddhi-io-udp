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

package io.siddhi.extension.io.udp.transport.config;

/**
 * Overrides the io.siddhi.tcp server configuration as this module is using some of it's classes.
 */
public class UDPServerConfig extends io.siddhi.extension.io.tcp.transport.config.ServerConfig {

    public static final int DEFAULT_UDP_PORT = 5556;

    public UDPServerConfig() {
        super();
        this.setPort(DEFAULT_UDP_PORT);
        this.setHost("*");
    }
}
