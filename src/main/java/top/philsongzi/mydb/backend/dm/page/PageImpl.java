package top.philsongzi.mydb.backend.dm.page;

import top.philsongzi.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * page 页面
 *
 * @author 小子松
 * @since 2023/8/6
 */
public class PageImpl implements Page{

    // 上一节实现了通用缓存框架，这一节我们将使用它来实现一个页面缓存。
    // 首先，定义出页面的结构:
    // 2. pageNunber 是这个页面的页号，从 1 开始。
    private int pageNumber;
    // 1. data 是这个页实际包含的字节数据
    private byte[] data;
    // 3. dirty 表示这个页面是否是脏页面，在缓存驱逐的时候，脏页面需要被写回磁盘。
    private boolean dirty;
    private Lock lock;
    // 4. PageCache 方便在拿到对 Page 的引用时可以快速对页面的缓存进行释放操作。
    private PageCache pageCache;

    // 构造方法
    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
        lock = new ReentrantLock();
    }

    // 实现接口方法——上锁、解锁、释放、设置脏页面、判断是否是脏页面、获取页号、获取数据
    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pageCache.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
