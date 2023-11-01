package top.philsongzi.mydb.backend.dm.pageCache;

import top.philsongzi.mydb.backend.common.AbstractCache;
import top.philsongzi.mydb.backend.dm.page.Page;
import top.philsongzi.mydb.backend.dm.page.PageImpl;
import top.philsongzi.mydb.backend.utils.Panic;
import top.philsongzi.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面缓存的具体实现类，需要继承抽象缓存框架，并且实现 getForCache() 和 releaseForCache() 两个抽象方法
 *
 * @author 小子松
 * @since 2023/8/6
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    // 字段
    private static final int MEM_MIN_LIM = 10;  // 最小内存限制
    public static final String DB_SUFFIX = ".db";  // 数据库文件后缀，public修饰，因为需要直接访问
    private RandomAccessFile file;  // 文件
    private FileChannel fc;  // 文件通道
    private Lock fileLock;  // 文件锁
    // 页面数，记录当前打开的数据库文件有多少页，在数据库文件被打开时就会被计算，新建页面时自增
    private AtomicInteger pageNumbers;

    // 构造方法
    PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResource) {
        // 调用父类构造方法——
        super(maxResource);
        // 如果最大资源数小于最小内存限制，抛出异常
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 初始化字段
        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);  // 根据文件大小计算页面数
    }

    // PageCache 还使用了一个 AtomicInteger，来记录了当前打开的数据库文件有多少页
    // 这个数字在数据库文件被打开时就会被计算，并在新建页面时自增。
    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);
        return pgno;
    }

    @Override
    public Page getPage(int pageNumber) throws Exception {
        return get(pageNumber);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    /**
     * 从文件系统中获取页面数据:由于数据源就是文件系统， getForCache 直接从文件中获取数据，包裹成Page即可。
     *
     * @param key 页面号
     * @return 页面
     * @throws Exception 异常
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        // 将数据包裹成Page
        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 释放页面数据：releaseForCache() 驱逐页面时，也只需要根据页面是否是脏页面，来决定是否需要写回文件系统
     * @param page
     */
    @Override
    protected void releaseForCache(Page page) {
        if(page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }

    private void flush(Page page) {
        int pgno = page.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    private static long pageOffset(int pgno) {
        // 页号从 1 开始，所以需要减 1
        return (pgno - 1) * PAGE_SIZE;
    }
}
