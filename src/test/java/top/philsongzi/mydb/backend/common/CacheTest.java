package top.philsongzi.mydb.backend.common;

import org.junit.Test;

import top.philsongzi.mydb.common.Error;
import top.philsongzi.mydb.backend.utils.Panic;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * CacheTest 缓存测试类。
 *
 * @author 小子松
 * @since 2023/8/4
 */
public class CacheTest {

    static Random random = new SecureRandom();
    private CountDownLatch cdl;
    private MockCache cache;

    @Test
    public void testCache() {
        cache = new MockCache();
        cdl = new CountDownLatch(200);
        for(int i = 0; i < 200; i ++) {
            Runnable r = () -> work();
            new Thread(r).run();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void work() {
        for (int i = 0; i < 1000; i++) {
            long uid = random.nextInt();
            long h = 0;
            try {
                h = cache.get(uid);
            } catch (Exception e) {
                if (e == Error.CacheFullException) continue;
                Panic.panic(e);
            }
            assert h == uid;
            cache.release(h);
        }
    }
}
