package top.philsongzi.mydb.backend.vm;

import top.philsongzi.mydb.backend.common.AbstractCache;
import top.philsongzi.mydb.backend.dm.DataManager;
import top.philsongzi.mydb.backend.tm.TransactionManager;
import top.philsongzi.mydb.backend.tm.TransactionManagerImpl;
import top.philsongzi.mydb.backend.utils.Panic;
import top.philsongzi.mydb.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * VM实现类：
 * 继承了 AbstractCache ——即被设计为 Entry 的缓存，需重写获取缓存和从缓存释放的方法。
 *
 * @author 小子松
 * @since 2023/8/9
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    // read() 方法读取一个 entry，注意判断下可见性
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        // 从 activeTransaction 中获取当前事务
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        // 从缓存中获取 entry
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        // 判断可见性
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    // insert() 则是将数据包裹成 Entry，交给 DM 插入
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    // delete 方法，实际上主要是前置的三件事：一是可见性判断，二是获取资源的锁，三是版本跳跃判断。删除的操作只有一个设置 XMAX。
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            // 1.可见性判断
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            // 2.锁获取
            Lock l = null;
            try {
                l = lt.add(xid, uid);
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();  // 阻塞在这一步，等待
                l.unlock();
            }

            if(entry.getXmax() == xid) {
                return false;
            }

            // 3.版本跳跃判断
            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            // 删除操作
            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    // begin() 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    // commit() 方法提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    // abort 事务的方法则有两种，手动和自动。
    // 手动指的是调用 abort() 方法，而自动，则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) {return;}
        lt.remove(xid);
        tm.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    // getForCache 实现：从 DM 中获取 Entry
    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    // releaseForCache 实现：从缓存中释放 Entry
    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

}
