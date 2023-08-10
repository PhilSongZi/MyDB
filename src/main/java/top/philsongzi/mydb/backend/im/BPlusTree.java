package top.philsongzi.mydb.backend.im;

import top.philsongzi.mydb.backend.dm.DataManager;
import top.philsongzi.mydb.backend.dm.dataItem.DataItem;
import top.philsongzi.mydb.backend.tm.TransactionManagerImpl;
import top.philsongzi.mydb.backend.utils.Parser;

import java.util.concurrent.locks.Lock;

/**
 * 数据库索引的 B+ 树实现。
 *
 * @author 小子松
 * @since 2023/8/10
 */
public class BPlusTree {

    // 需要维护的引用：数据管理器、根节点 UID、根节点数据项、根节点数据项的锁
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;

    // B+ 树的创建
    public static long create(DataManager dm) throws Exception {
        byte[] rawData = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawData);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }
}
