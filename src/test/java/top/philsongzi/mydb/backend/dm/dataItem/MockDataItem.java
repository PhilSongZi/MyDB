package top.philsongzi.mydb.backend.dm.dataItem;

import top.philsongzi.mydb.backend.common.SubArray;
import top.philsongzi.mydb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DataItem 的模拟实现
 *
 * @author 小子松
 * @since 2023/10/17
 */
public class MockDataItem implements DataItem {

    private SubArray data;
    private byte[] oldData;
    private long uid;
    private Lock rLock;
    private Lock wLock;

    public static MockDataItem newMockDataItem(long uid, SubArray data) {
        MockDataItem di = new MockDataItem();
        di.data = data;
        di.oldData = new byte[data.end - data.start];
        di.uid = uid;
        ReadWriteLock l = new ReentrantReadWriteLock();
        di.rLock = l.readLock();
        di.wLock = l.writeLock();
        return di;
    }

    @Override
    public SubArray data() {
        return data;
    }

    @Override
    public void before() {
        wLock.lock();
        System.arraycopy(data.raw, data.start, oldData, 0, oldData.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldData, 0, data.raw, data.start, oldData.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        wLock.unlock();
    }

    @Override
    public void release() {}

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
        return null;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldData;
    }

    @Override
    public SubArray getRaw() {
        return data;
    }
}
