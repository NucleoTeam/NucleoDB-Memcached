package svs.memcached.server;

import com.nucleodb.library.NucleoDB;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;

import static com.nucleodb.library.NucleoDB.DBType.NO_LOCAL;

/**
 *
 * Netty based Memcached server
 *
 * Netty 4 has support for memcache protocol, unfortunately it only supports binary
 * So implementing a simple prototype here
 *
 * Created by ssmirnov on 2/4/17.
 *
 *
 */
public class MemcachedServer {

    private static final Logger logger = LogManager.getLogger(MemcachedServer.class);

    private static final int DEFAULT_PORT = 11211;

    // cache params
    private static final int CACHE_MAX_SIZE = 1000000;
    private static final int CACHE_MAX_IDLE_TIME_MS = 600000; // 10 min max idle
    private static final int CACHE_CONCURRENCY_LEVEL = 32; // set higher concurrency level (default is 4)
    private static final boolean CACHE_EXPIRE_ON_MEMORY_PRESSURE = true;

    private static final int WORKER_THREADS_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private static final int SO_BACKLOG_VALUE = 128;

    private final int port;

    public MemcachedServer(int port) {
        this.port = port;
    }

    public static void main(String[] args) {
        final int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = DEFAULT_PORT;
        }
        logger.info("Starting server on port {}", port);
        MemcachedServer server = new MemcachedServer(port);
        try {
            server.start("kafka");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IncorrectDataEntryClassException e) {
            throw new RuntimeException(e);
        } catch (MissingDataEntryConstructorsException e) {
            throw new RuntimeException(e);
        }
    }

    public void start(String type) throws InterruptedException, IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
        logger.info("Bootstrapping Memcached Server");
        String kafkaHosts = System.getenv().getOrDefault("KAFKA_HOSTS", "127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "memcache");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        // business logic thread pool
        // It is probably overkill to use separate thread pool for cache access here but
        // generally it is good practice to separate IO from business logic
        final EventExecutorGroup mainGroup = new DefaultEventExecutorGroup(WORKER_THREADS_COUNT);
        try {
            NucleoDB nucleoDB = switch(type){
                case "local"-> new NucleoDB(
                    NO_LOCAL,
                    c-> c.getConnectionConfig().setMqsConfiguration(new LocalConfiguration()),
                    c-> c.getDataTableConfig().setMqsConfiguration(new LocalConfiguration()),
                    new String[]{"com.nucleodb.memcache.data"}
                );
                case "kafka"-> new NucleoDB(
                    NO_LOCAL,
                    new String[]{"com.nucleodb.memcache.data"}
                );
                default -> throw new IllegalStateException("Unexpected value: " + type);
            };
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup);
            bootstrap.channel(NioServerSocketChannel.class);

            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(new MemcacheDecoder());
                    pipeline.addLast(new MemcacheEncoder());
                    // Cache Operations Command Handler
                    pipeline.addLast(mainGroup, "commandHandler", new MemcacheCommandHandler(nucleoDB));
                }
            });

            bootstrap.option(ChannelOption.SO_BACKLOG, SO_BACKLOG_VALUE);
            bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = bootstrap.bind(port).sync();
            logger.info("Server is ready <port={}>...", port);
            f.channel().closeFuture().sync();
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } finally {
            logger.info("Shutting down server...");
            mainGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

}
