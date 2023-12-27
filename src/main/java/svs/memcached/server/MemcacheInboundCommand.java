package svs.memcached.server;

import com.nucleodb.memcache.data.KeyVal;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 * Memcache Inbound Command
 * It is decoded memcache command
 *
 * Created by ssmirnov on 2/4/17.
 */
public class MemcacheInboundCommand {

    private final String key;
    private final KeyVal value;
    private final CommandType type;


    private MemcacheInboundCommand(@Nonnull String key, @Nullable KeyVal value, @Nonnull CommandType type){
        this.key = key;
        this.value = value;
        this.type = type;
    }

    public static MemcacheInboundCommand newSetCommand(String key, String data, int flags, int targetTimeSec) {
        return new MemcacheInboundCommand(key, new KeyVal(data, flags, targetTimeSec), CommandType.SET);
    }

    public static MemcacheInboundCommand newGetCommand(String key) {
        return new MemcacheInboundCommand(key, null, CommandType.GET);
    }
    public static MemcacheInboundCommand newDeleteCommand(String key) {
        return new MemcacheInboundCommand(key, null, CommandType.DELETE);
    }

    public String getKey() {
        return key;
    }

    public KeyVal getValue() {
        return value;
    }

    public CommandType getType() {
        return type;
    }


    @Override
    public String toString() {
        return "MemcacheInboundCommand{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
