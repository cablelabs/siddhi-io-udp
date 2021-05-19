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
        byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(0, bytes);
//        ctx.write(bytes);

//        Object object = bytes;
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
//        final Attribute.Type[] types = {Attribute.Type.OBJECT};
//        final Event[] events = SiddhiEventConverter.toConvertToSiddhiEvents(byteBuffer, types);

//        objects[0] = bytes;

//        ctx.write(byteBuffer);
        if (sourceEventListener != null) {
            log.info("Sending out bytes to the Siddhi sourceEventListener");
            try {
//                Event[] events = {new Event(System.currentTimeMillis(), byteBuffer)};
//                sourceEventListener.onEvent(new Event(System.currentTimeMillis(), byteBuffer), new String[0]);
//                sourceEventListener.onEvent(events, new String[0]);
//                sourceEventListener.onEvent(events, null);
//                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), types));
                sourceEventListener.onEvent(byteBuffer, null);
//                sourceEventListener.onEvent(events, null);
            } catch (BufferUnderflowException e) {
                log.error("Unexpected error sending sourceEventListener", e);
            }
        }
    }
}
