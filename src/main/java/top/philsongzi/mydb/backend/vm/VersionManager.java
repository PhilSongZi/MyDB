package top.philsongzi.mydb.backend.vm;

import top.philsongzi.mydb.backend.dm.DataManager;
import top.philsongzi.mydb.backend.tm.TransactionManager;

/**
 * VersionManager 接口定义。向上层提供功能
 *
 * @author 小子松
 * @since 2023/8/9
 */
public interface VersionManager {

    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
