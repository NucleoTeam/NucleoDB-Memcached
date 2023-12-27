package svs.memcached.server;

import com.nucleodb.memcache.data.KeyVal;
import javax.annotation.Nullable;

/**
 * Memcache Outbound Command
 * Represents the result of processed MemcacheInboundCommand
 *
 * Created by ssmirnov on 2/4/17.
 */
public class MemcacheOutboundCommand {

    private final CommandType type;
    private final String key;
    private final KeyVal value;

    private MemcacheOutboundCommand(@Nullable CommandType type, @Nullable String key, @Nullable KeyVal value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    /**
     * Creates new Outbound Response Command for successful Get operation
     * @param command inbound Get command
     * @param value value obtained from the cache
     * @return constructed MemcacheOutboundCommand
     */
    public static MemcacheOutboundCommand newGetCommandResult(MemcacheInboundCommand command, KeyVal value) {
        return new MemcacheOutboundCommand(CommandType.GET, command.getKey(), value);
    }

    /**
     * Creates new Outbound Response Command for successful Set operation
     * @return constructed MemcacheOutboundCommand
     */
    public static MemcacheOutboundCommand newSetCommandResult() {
        return new MemcacheOutboundCommand(CommandType.SET, null, null);
    }

    public static MemcacheOutboundCommand newDeleteCommandResult() {
        return new MemcacheOutboundCommand(CommandType.DELETE, null, null);
    }
    public static MemcacheOutboundCommand newNotFoundCommandResult() {
        return new MemcacheOutboundCommand(CommandType.NOT_FOUND, null, null);
    }


    public CommandType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public KeyVal getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "MemcacheOutboundCommand{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
