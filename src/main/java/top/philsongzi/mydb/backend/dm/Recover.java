package top.philsongzi.mydb.backend.dm;

import com.google.common.primitives.Bytes;
import top.philsongzi.mydb.backend.common.SubArray;
import top.philsongzi.mydb.backend.dm.dataItem.DataItem;
import top.philsongzi.mydb.backend.dm.logger.Logger;
import top.philsongzi.mydb.backend.dm.page.Page;
import top.philsongzi.mydb.backend.dm.page.PageX;
import top.philsongzi.mydb.backend.dm.pageCache.PageCache;
import top.philsongzi.mydb.backend.tm.TransactionManager;
import top.philsongzi.mydb.backend.utils.Panic;
import top.philsongzi.mydb.backend.utils.Parser;

import java.util.*;

/**
 * 数据恢复策略——
 * 日志策略：在进行 I 和 U 操作之前，必须先进行对应的日志操作，在保证日志写入磁盘后，才进行数据操作。
 * 规定1：正在进行的事务，不会读取其他任何未提交的事务产生的数据。
 * 规定2：正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据。
 * 日志恢复：
 * 1. 重做所有崩溃时已完成（committed 或 aborted）的事务
 * 2. 撤销所有崩溃时未完成（active）的事务
 * 在恢复后，数据库就会恢复到所有已完成事务结束，所有未完成事务尚未开始的状态。
 *
 * @author 小子松
 * @since 2023/8/7
 */
public class Recover {

    // 两种日志的格式——
    // updateLog:
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    // insertLog:
    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 恢复数据库
     * @param tm 事务管理器
     * @param lg 日志 logger
     * @param pc page cache
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /**
     * 重做所有崩溃时已完成（committed 或 aborted）的事务
     * @param tm 事务管理器
     * @param lg 日志 logger
     * @param pc page cache
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    /**
     * 撤销所有崩溃时未完成（active）的事务
     * @param tm 事务管理器
     * @param lg 日志 logger
     * @param pc page cache
     */
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for(Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // updateLog： [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    /**
     * 执行日志的重做或者撤销
     * @param pc page cache
     * @param log 日志
     * @param flag 重做或者撤销
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    // insertLog：[LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    /**
     * 执行日志的重做或者撤销
     * @param pc page cache
     * @param log 日志
     * @param flag 重做或者撤销
     */
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                // 撤销时，将数据项标记为无效，是一种逻辑删除
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}