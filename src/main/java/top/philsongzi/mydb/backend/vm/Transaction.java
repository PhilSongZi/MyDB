package top.philsongzi.mydb.backend.vm;

import top.philsongzi.mydb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * VM对一个事务的抽象
 *
 * @author 小子松
 * @since 2023/8/9
 */
public class Transaction {

    // 事务 ID、隔离级别、快照
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    /**
     * 事务的构造方法
     * @param xid 事务 ID
     * @param level 隔离级别
     * @param active 当前活跃的事务
     * @return 事务对象
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    /**
     * 判断一个事务是否在快照中
     * @param xid 事务 ID
     * @return 是否在快照中
     */
    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
