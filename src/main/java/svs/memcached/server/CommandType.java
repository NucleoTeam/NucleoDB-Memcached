package svs.memcached.server;

/**
 * Type of supported memcache commands
 *
 * Created by ssmirnov on 2/5/17.
 */
public enum CommandType {
    GET,
    SET,
    DELETE,
    NOT_FOUND,
}
