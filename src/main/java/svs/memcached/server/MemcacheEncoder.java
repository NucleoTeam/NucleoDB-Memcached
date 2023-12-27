package svs.memcached.server;

import com.nucleodb.memcache.data.KeyVal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


/**
 *
 * Encoder of memcache response messages
 *
 * Created by ssmirnov on 2/4/17.
 *
 */
public class MemcacheEncoder extends MessageToByteEncoder<MemcacheOutboundCommand> {

    private static final Logger logger = LogManager.getLogger(MemcacheEncoder.class);

    private static final String R_N_STRING = "\r\n";
    private static final byte[] R_N_BYTES = R_N_STRING.getBytes(Charset.defaultCharset());
    private static final byte[] END_BYTES = "END\r\n".getBytes(Charset.defaultCharset());
    private static final byte[] STORED_BYTES = "STORED\r\n".getBytes(Charset.defaultCharset());
    private static final byte[] NOT_FOUND = "NOT_FOUND\r\n".getBytes(Charset.defaultCharset());
    private static final byte[] DELETED = "DELETED\r\n".getBytes(Charset.defaultCharset());


    //    means some sort of client error in the input line, i.e. the input
    //    doesn't conform to the protocol in some way. <error> is a
    //    human-readable error string.
    private static final String CLIENT_ERROR = "CLIENT_ERROR ";

    //    means the client sent a nonexistent command name.
    private static final String UNSUPPORTED_COMMAND_ERROR = "ERROR\r\n";

    //    means some sort of server error prevents the server from carrying
    //    out the command. <error> is a human-readable error string. In cases
    //    of severe server errors, which make it impossible to continue
    //    serving the client (this shouldn't normally happen), the server will
    //            close the connection after sending the error line. This is the only
    //            case in which the server closes a connection to a client.
    private static final String SERVER_ERROR = "SERVER_ERROR ";


    @Override
    protected void encode(ChannelHandlerContext ctx, MemcacheOutboundCommand msg, ByteBuf out) throws Exception {
        logger.debug("Encoding command: {}", msg);
        switch (msg.getType()) {
            case GET: {
                KeyVal value = msg.getValue();
                if (value != null){
                    byte[] bytes = value.getData().getBytes(StandardCharsets.UTF_8);
                    out.writeBytes(String.format("VALUE %s %d %d\r\n",
                            msg.getKey(), value.getFlags(), bytes.length).getBytes());
                    out.writeBytes(bytes);
                    out.writeBytes(R_N_BYTES);
                }
                out.writeBytes(END_BYTES);
                break;
            }
            case SET: {
                out.writeBytes(STORED_BYTES);
                break;
            }
            case DELETE: {
                out.writeBytes(DELETED);
                break;
            }
            case NOT_FOUND: {
                out.writeBytes(NOT_FOUND);
                break;
            }
            default: {
                throw new IllegalArgumentException("Unsupported Command type: " + msg.getType());
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Error processing command", cause);
        if (ctx.channel().isOpen()) {
            if (cause instanceof DecodingException) {
                ctx.channel().writeAndFlush(CLIENT_ERROR + cause.getCause().getMessage() + R_N_STRING);
            } else if (cause instanceof UnsupportedCommandException) {
                ctx.channel().writeAndFlush(UNSUPPORTED_COMMAND_ERROR);
            } else {
                ctx.channel().writeAndFlush(SERVER_ERROR + cause.getMessage() + R_N_STRING);
                ctx.close();
            }
        }
    }
}
