package top.philsongzi.mydb.backend.tm;

import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TM测试类——不写单测一时爽，改bug火葬场。
 *
 * @author 小子松
 * @since 2023/8/3
 */
public class TransactionManagerTest {

    static Random random = new SecureRandom();
    private int transCnt = 0;  // 事务计数
    private int noWorkers = 50;  // 工作线程数
    private int noWorks = 3000;  // 每个工作线程的工作次数
    private TransactionManager tranmanager;
    private Lock lock = new ReentrantLock();  // 锁，用于线程同步
    private Map<Long, Byte> transMap;  // 事务状态映射，存事务ID和状态
    private CountDownLatch cdl;  // 用于控制线程并发执行

    @Test
    public void testMultiThread() {

        tranmanager = TransactionManager.create("./tmp/tranmger_test");
        transMap = new ConcurrentHashMap<>();
        cdl = new CountDownLatch(noWorkers);
        for (int i = 0; i < noWorkers; i++) {
            Runnable r = () -> worker();
            new Thread(r).run();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 测试报错，看看Java assert关键字的用法
        // java.lang.AssertionError
        // at top.philsongzi.mydb.backend.tm.TransactionManagerTest.testMultiThread(TransactionManagerTest.java:45)
        // 创建一个表示文件的 file 对象，调用 delete() 方法删除文件，使用断言判断删除结果。
        // 注意：断言默认关闭！！需要在运行时加上 -ea 参数开启断言。
        assert new File("./tmp/tranmger_test.xid").delete();
    }

    private void worker() {
        boolean inTrans = false;
        long transXID = 0;
        for(int i = 0; i < noWorks; i ++) {
            int op = Math.abs(random.nextInt(6));
            if(op == 0) {
                lock.lock();
                // 如果不在事务中，则开始一个新的事务，并将事务ID和状态存入 transMap 中，增加 transCnt 的计数
                if(inTrans == false) {
                    long xid = tranmanager.begin();
                    transMap.put(xid, (byte)0);
                    transCnt ++;
                    transXID = xid;
                    inTrans = true;
                } else {  // 否则，随机选择提交或回滚当前事务，并更新 transMap 和 inTrans 的状态
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch(status) {
                        case 1:
                            tranmanager.commit(transXID);
                            break;
                        case 2:
                            tranmanager.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);
                    inTrans = false;
                }
                lock.unlock();
            } else {
                lock.lock();
                // 如果当前存在事务，则随机选择一个事务ID，获取状态，并根据状态判断事务是否处于活跃、已提交或已回滚状态
                if(transCnt > 0) {
                    long xid = (long)((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tranmanager.isActive(xid);
                            break;
                        case 1:
                            ok = tranmanager.isCommitted(xid);
                            break;
                        case 2:
                            ok = tranmanager.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
        cdl.countDown();
    }
}
