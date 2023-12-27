package com.nucleodb.memcache;

import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import svs.memcached.server.MemcachedServer;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;


class NDBMemcacheTest{
  static MemcachedServer memcache = null;

  @BeforeEach
  void setUp() {
    if(memcache==null) {
      memcache = new MemcachedServer(11211);
      new Thread(() -> {
        try {
          memcache.start("local");
        } catch (MissingDataEntryConstructorsException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (IncorrectDataEntryClassException e) {
          throw new RuntimeException(e);
        }
      }).start();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  void testCreateEntry() throws IOException {
    MemcachedClient memcachedClient = new MemcachedClient(
        AddrUtil.getAddresses("127.0.0.1:11211")
    );
    assertNull(memcachedClient.get("test"));
    memcachedClient.set("test", 2, "TestValue");
    assertEquals("TestValue", memcachedClient.get("test"));
  }

  @Test
  void testModifyEntry() throws IOException {
    MemcachedClient memcachedClient = new MemcachedClient(
        AddrUtil.getAddresses("127.0.0.1:11211")
    );
    assertNull(memcachedClient.get("modifytest"));
    memcachedClient.set("modifytest", 2, "TestValue");
    assertEquals("TestValue", memcachedClient.get("modifytest"));
    memcachedClient.set("modifytest", 23, "newVal");
    assertEquals("newVal", memcachedClient.get("modifytest"));
  }

  @Test
  void testDeleteEntry() throws IOException, InterruptedException, ExecutionException {
    MemcachedClient memcachedClient = new MemcachedClient(
        AddrUtil.getAddresses("127.0.0.1:11211")
    );
    assertNull(memcachedClient.get("deletetest"));
    memcachedClient.set("deletetest", 2, "TestValue");
    assertEquals("TestValue", memcachedClient.get("deletetest"));
    OperationFuture<Boolean> deletetest = memcachedClient.delete("deletetest");
    CountDownLatch countDownLatch = new CountDownLatch(1);
    deletetest.addListener(future -> countDownLatch.countDown());
    countDownLatch.await();
    assertTrue(deletetest.get().booleanValue());
    assertNull(memcachedClient.get("deletetest"));
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    deletetest = memcachedClient.delete("deletetest");
    deletetest.addListener(future -> countDownLatch2.countDown());
    countDownLatch2.await();
    assertFalse(deletetest.get().booleanValue());
  }
}