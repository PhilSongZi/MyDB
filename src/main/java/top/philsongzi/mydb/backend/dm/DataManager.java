package top.philsongzi.mydb.backend.dm;

import top.philsongzi.mydb.backend.dm.dataItem.DataItem;
import top.philsongzi.mydb.backend.dm.logger.Logger;
import top.philsongzi.mydb.backend.dm.page.PageOne;
import top.philsongzi.mydb.backend.dm.pageCache.PageCache;
import top.philsongzi.mydb.backend.tm.TransactionManager;

/**
 * DataManager 接口
 *
 * @author 小子松
 * @since 2023/8/7
 */
public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /**
     * 从空文件创建 DataManager
     * @param path 文件路径
     * @param mem 内存大小
     * @param tm 事务管理器
     * @return DataManager
     */
    static DataManager create(String path, long mem, TransactionManager tm) {
        // 创建 PageCache 和 Logger，用的 create 方法
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 调用初始化方法：空文件创建首先需要对第一页进行初始化
        dm.initPageOne();
        return dm;
    }

    /**
     * 打开已有文件创建 DataManager
     * @param path 文件路径
     * @param mem 内存大小
     * @param tm 事务管理器
     * @return DataManager
     */
    static DataManager open(String path, long mem, TransactionManager tm) {
        // 创建 PageCache 和 Logger，用的 open 方法
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 调用 loadCheckPageOne 方法：对第一页进行校验，来判断是否需要执行恢复流程
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pageCache.flushPage(dm.pageOne);

        return dm;
    }
}