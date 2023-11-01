package top.philsongzi.mydb.backend.vm;

import com.google.common.primitives.Bytes;

import top.philsongzi.mydb.backend.common.SubArray;
import top.philsongzi.mydb.backend.dm.dataItem.DataItem;
import top.philsongzi.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * Entry：记录——DM 层向上层提供了数据项（Data Item）的概念，VM 通过管理所有的数据项，向上层提供了记录（Entry）的概念。
 * 上层模块通过 VM 操作数据的最小单位，就是记录。
 *
 * @author 小子松
 * @since 2023/8/9
 */
public class Entry {

    /*
      对于一条记录来说，MYDB 使用 Entry 类维护了其结构。虽然理论上，MVCC 实现了多版本，
      但是在实现中，VM 并没有提供 Update 操作，对于字段的更新操作由后面的表和字段管理（TBM）实现。
      所以在 VM 的实现中，一条记录只有一个版本。
     */
    // Entry 结构：[OF_XMIN][OF_XMAX][OF_DATA] 分别是 创建该条记录（版本）的事务编号、删除（更新）该条记录（版本）的事务编号、这条记录持有的数据
    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    // 一条记录保存在一条 DataItem 中，所以 Entry 中保存一个 DataItem 的引用。
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl) vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    /**
     * Entry 结构：[XMIN][XMAX][DATA] 分别是 创建该条记录（版本）的事务编号、删除该条记录（版本）的事务编号、这条记录持有的数据
     * 创建 Entry 时，调用本方法。
     * @param xid
     * @param data
     * @return
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        ((VersionManagerImpl) vm).releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }

    /**
     * 获取记录中持有的数据，按照 Entry 结构来解析。以拷贝的方式返回数据。
     * @return
     */
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.data();
            byte[] data = new byte[subArray.end - subArray.start - OF_DATA];
            System.arraycopy(subArray.raw, subArray.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 修改 XMAX
     * @param xid
     */
    public void setXmax(long xid) {
        // before 和 after 是 DataItem 中定义好的数据项修改规则
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    public void setXmin(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMIN, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getUid() {
        return uid;
    }
}
