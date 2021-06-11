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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.siddhi.core.stream.input.source.SourceEventListener;
import org.apache.log4j.Logger;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Handles the incoming packets.
 */
public final class IncomingPacketHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private static final Logger log = Logger.getLogger(UDPNettyServer.class);
    private int packetCount = 0;
    private final SourceEventListener sourceEventListener;

    public IncomingPacketHandler(final SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
    }

    public int getPacketCount() {
        return packetCount;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.trace("Inside channelRead");
        this.channelRead0(ctx, (DatagramPacket) msg);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket packet) {
        log.trace("Inside channelRead0");
        packetCount++;

        final ByteBuf buf = packet.content();
        final byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(0, bytes);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        if (sourceEventListener != null) {
            log.info("Sending out bytes to the Siddhi sourceEventListener");
            try {
                sourceEventListener.onEvent(byteBuffer.array(), null);
            } catch (BufferUnderflowException e) {
                log.error("Unexpected error sending sourceEventListener", e);
            }
        }
    }
}
