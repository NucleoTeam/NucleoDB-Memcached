package svs.memcached.server;

import com.nucleodb.library.NucleoDB;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.utils.Pagination;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.memcache.data.KVDataEntry;
import com.nucleodb.memcache.data.KeyVal;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * Main Command handler
 * Performs actual cache operations
 *
 * Created by ssmirnov on 2/4/17.
 */
public class MemcacheCommandHandler extends SimpleChannelInboundHandler<MemcacheInboundCommand> {


    private final NucleoDB nucleoDB;

    private static final Logger logger = LogManager.getLogger(MemcacheCommandHandler.class);

    public MemcacheCommandHandler(NucleoDB nucleoDB) {
        this.nucleoDB = nucleoDB;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MemcacheInboundCommand command) throws Exception {
        logger.debug("Processing command: {}", command);

        switch (command.getType()) {
            case GET: {
                Serializer.log(command.getKey());
                Set<KVDataEntry> values = nucleoDB.getTable(KeyVal.class).get("id", command.getKey(), new DataEntryProjection(new Pagination(0,1)){{
                    setWritable(true);
                }}).stream().map(e->(KVDataEntry)e).collect(Collectors.toSet());
                if(values.size()>0) {
                    KVDataEntry value = values.stream().findFirst().get();
                    if (value != null && value.getData().getTargetTimeSec() != 0 && value.getData().getTargetTimeSec() * 1000l < System.currentTimeMillis()) {
                        value = null; // expired
                        nucleoDB.getTable(KeyVal.class).deleteSync(value); // TODO synchronization for concurrent access
                    }
                    ctx.writeAndFlush(MemcacheOutboundCommand.newGetCommandResult(command, value.getData()));
                }else{
                    ctx.writeAndFlush(MemcacheOutboundCommand.newGetCommandResult(command, null));
                }
                break;
            }
            case SET: {
                Set<KVDataEntry> values = nucleoDB.getTable(KeyVal.class).get("id", command.getKey(), new DataEntryProjection(new Pagination(0,1)){{
                    setWritable(true);
                }}).stream().map(e->(KVDataEntry)e).collect(Collectors.toSet());
                if (command.getValue().getTargetTimeSec() < 0) { // If a negative value is given the item is immediately expired.
                    if(values.size()>0) {
                        KVDataEntry value = values.stream().findFirst().get();
                        nucleoDB.getTable(KeyVal.class).deleteSync(value);
                    }
                } else {
                    if(values.size()>0) {
                        KVDataEntry value = values.stream().findFirst().get();
                        value.getData().setData(command.getValue().getData());
                        value.getData().setTargetTimeSec(command.getValue().getTargetTimeSec());
                        nucleoDB.getTable(KeyVal.class).saveSync(value);
                    }else{
                        command.getValue().setKey(command.getKey());
                        nucleoDB.getTable(KeyVal.class).saveSync(new KVDataEntry(command.getValue()));
                    }
                }
                ctx.writeAndFlush(MemcacheOutboundCommand.newSetCommandResult());
                break;
            }
            case DELETE: {
                Set<KVDataEntry> values = nucleoDB.getTable(KeyVal.class).get("id", command.getKey(), new DataEntryProjection(new Pagination(0,1)){{
                    setWritable(true);
                }}).stream().map(e->(KVDataEntry)e).collect(Collectors.toSet());
                if(values.size()>0) {
                    KVDataEntry value = values.stream().findFirst().get();
                    Serializer.log("DELETING "+value.getKey());
                    nucleoDB.getTable(KeyVal.class).deleteSync(value);
                    ctx.writeAndFlush(MemcacheOutboundCommand.newDeleteCommandResult());
                }else{
                    ctx.writeAndFlush(MemcacheOutboundCommand.newNotFoundCommandResult());
                }
                break;
            }
            default : {
                throw new IllegalArgumentException("Unsupported command type: " + command.getType());
            }
        }
    }
}
