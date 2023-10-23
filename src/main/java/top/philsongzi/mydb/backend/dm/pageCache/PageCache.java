package top.philsongzi.mydb.backend.dm.pageCache;

import top.philsongzi.mydb.backend.dm.page.Page;
import top.philsongzi.mydb.backend.utils.Panic;
import top.philsongzi.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 页面缓存的接口
 *
 * @author 小子松
 * @since 2023/8/6
 */
public interface PageCache {

    // 1 << 13 = 8192 = 8KB 页面大小
    int PAGE_SIZE = 1 << 13;  // public static final 对于接口类成员来说是多余的，因为接口类成员默认就是 public static final 的。

    int newPage(byte[] initData);
    Page getPage(int pageNumber) throws Exception;
    void close();
    void release(Page page);

    void truncateByBgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page page);

    // public 修饰对于接口类方法来说是多余的，因为接口类方法默认就是 public 的。
    static PageCacheImpl create(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        // Duplicated code fragment (15 lines long).
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
