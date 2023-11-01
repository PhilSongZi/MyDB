package top.philsongzi.mydb.backend.dm.dataItem;

import top.philsongzi.mydb.backend.common.SubArray;
import top.philsongzi.mydb.backend.dm.DataManagerImpl;
import top.philsongzi.mydb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DataItem 接口实现类。
 * DataItem 中保存的数据，结构如下：
 * [ValidFlag] [DataSize] [Data]
 * 其中 ValidFlag 占用 1 字节，标识了该 DataItem 是否有效。删除一个 DataItem，只需要简单地将其有效位设置为 0。
 * DataSize 占用 2 字节，标识了后面 Data 的长度。
 * @author 小子松
 * @since 2023/8/7
 */
public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    // 共享内存数组，该数组的结构包含：原始数据、开始位置、结束位置
    private SubArray raw;
    private byte[] oldRaw;
    // 读写锁
    private Lock rLock;
    private Lock wLock;
    // 保存一个 dm 的引用是因为其释放依赖 dm 的释放（dm 同时实现了缓存接口，用于缓存 DataItem），以及修改数据时落日志。
    private DataManagerImpl dm;
    private long uid;
    private Page pg;  // 该 DataItem 所在的页面

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    public boolean isValid() {
        // row 是共享内存数组，raw.raw 是原始数据，raw.start 是开始位置，OF_VALID 是偏移量
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    /**
     * 获取 DataItem 中的数据。该方法返回的数组是数据共享的，而不是拷贝实现的，所以使用了 SubArray。
     * @return SubArray
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    // 上层模块对 DataItem 进行修改时，需遵循的流程：
    // 1. 修改前需要先调用 before() 方法，该方法会将原始数据拷贝到 oldRaw 中，以便于回滚。
    // 2. 想要撤销修改时，调用 unBefore() 方法，
    // 3. 在修改完成后，调用 after() 方法
    // 目的是——保存前相数据，及时落日志。DM会保证对 DataItem 的修改是原子的。
    /**
     * 1. 修改前需要先调用 before() 方法，该方法会将原始数据拷贝到 oldRaw 中，以便于回滚。
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 2. 想要撤销修改时，调用 unBefore() 方法，
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    /**
     * 3. 在修改完成后，调用 after() 方法
     */
    @Override
    public void after(long xid) {
        // 调用 DM 的方法，将修改的 DataItem 落日志
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    /**
     * 在使用完 DataItem 后，也应当及时调用 release() 方法，释放掉 DataItem 的缓存（由 DM 缓存 DataItem）。
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }

}

