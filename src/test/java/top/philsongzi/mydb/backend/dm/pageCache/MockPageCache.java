package top.philsongzi.mydb.backend.dm.pageCache;

import top.philsongzi.mydb.backend.dm.page.MockPage;
import top.philsongzi.mydb.backend.dm.page.Page;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面缓存的模拟实现
 * @author 小子松
 * @since 2023/8/6
 */
public class MockPageCache implements PageCache {

    private Map<Integer, MockPage> cache = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private AtomicInteger noPages = new AtomicInteger(0);

    @Override
    public int newPage(byte[] initData) {
        lock.lock();
        try {
            int pgno = noPages.incrementAndGet();
            MockPage pg = MockPage.newMockPage(pgno, initData);
            cache.put(pgno, pg);
            return pgno;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        lock.lock();
        try {
            return cache.get(pgno);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {}

    @Override
    public void release(Page page) {}

    @Override
    public void truncateByPgno(int maxPgno) {}

    @Override
    public int getPageNumber() {
        return noPages.intValue();
    }

    @Override
    public void flushPage(Page pg) {}

}

