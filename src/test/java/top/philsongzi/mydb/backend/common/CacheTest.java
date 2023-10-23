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
    private CountDownLatch cdl;  // 用于控制线程同步
    private MockCache cache;  // 模拟缓存对象

    @Test
    public void testCache() {
        cache = new MockCache();
        cdl = new CountDownLatch(200);  // 200 个线程
        for(int i = 0; i < 200; i ++) {
            Runnable r = () -> work();  // 200 个线程同时执行 work() 方法
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
            // 生成随机UID，模拟缓存的使用
            long uid = random.nextInt();
            long h = 0;
            try {
                h = cache.get(uid);  // 从缓存中获取缓存值
            } catch (Exception e) {
                if (e == Error.CacheFullException) continue;  // 如果缓存满了，就跳过
                Panic.panic(e);
            }
            assert h == uid;  // 断言缓存值与UID相等
            cache.release(h);
        }
    }
}
