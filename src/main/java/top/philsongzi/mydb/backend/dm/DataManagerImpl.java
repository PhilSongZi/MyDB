package top.philsongzi.mydb.backend.dm;

import top.philsongzi.mydb.backend.common.AbstractCache;
import top.philsongzi.mydb.backend.dm.dataItem.DataItem;
import top.philsongzi.mydb.backend.dm.dataItem.DataItemImpl;
import top.philsongzi.mydb.backend.dm.logger.Logger;
import top.philsongzi.mydb.backend.dm.page.Page;
import top.philsongzi.mydb.backend.dm.page.PageOne;
import top.philsongzi.mydb.backend.dm.page.PageX;
import top.philsongzi.mydb.backend.dm.pageCache.PageCache;
import top.philsongzi.mydb.backend.dm.pageIndex.PageIndex;
import top.philsongzi.mydb.backend.dm.pageIndex.PageInfo;
import top.philsongzi.mydb.backend.tm.TransactionManager;
import top.philsongzi.mydb.backend.utils.Panic;
import top.philsongzi.mydb.backend.utils.Types;
import top.philsongzi.mydb.common.Error;

/**
 * DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。
 * DataItem 存储的 key，是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
 *
 * @author 小子松
 * @since 2023/8/7
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    // DM 层提供给上层的三个功能：读取——根据 uid 从缓存中获取 DataItem，并校验有效位
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);  // 通用缓存框架的的 get 方法
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    // DM 层提供给上层的三个功能：插入、读取、修改
    // 其中，修改 通过读出的 DataItem 实现，所以， DM 只需要提供 read 和 insert 方法
    // insert() 方法，在 pageIndex 中获取一个足以存储插入内容的页面的页号，
    // 获取页面后，首先需要写入插入日志，
    // 接着才可以通过 pageX 插入数据，并返回插入位置的偏移。
    // 最后需要将页面信息重新插入 pageIndex。
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        // 尝试获取可用页
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            // 首先做日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            // 再执行插入操作，调用普通页面 PageX 的 insert 方法
            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    // DataManager 正常关闭时，需要执行缓存和日志的关闭流程，不要忘了设置第一页的字节校验：
    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);  // 设置第一页的字节校验
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    // DataItem 缓存，getForCache()，只需要从 key 中解析出页号，从 pageCache 中获取到页面，再根据偏移，解析出 DataItem 即可：
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    // DataItem 缓存释放，需要将 DataItem 写回数据源，由于对文件的读写是以 页 为单位进行的，只需要将 DataItem 所在的页 release 即可
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.initRaw());  // 创建新页 PageOne 中的 newPage 方法调用
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);  // 读取第一页, PageCache 中的 getPage 方法调用
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex，DataManager 被创建时，需要获取所有页面并填充 PageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            // 调用 add 方法，将 Page 的编号和空闲空间大小添加到 PageIndex 中
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            // 用完 Page 之后 release，避免撑爆缓存
            pg.release();
        }
    }

}

