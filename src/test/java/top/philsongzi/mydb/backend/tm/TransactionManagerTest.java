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
    private TransactionManager tmger;
    private Lock lock = new ReentrantLock();  // 锁，用于线程同步
    private Map<Long, Byte> transMap;  // 事务状态映射，存事务ID和状态
    private CountDownLatch cdl;  // 用于控制线程并发执行

    @Test
    public void testMultiThread() {
        // 搞了个绝对路径进去。。。
        // 因为写相对路径时报错：java.io.IOException: 系统找不到指定的路径。
        tmger = TransactionManager.create("D:\\open_source_projects\\MyDB\\src\\test\\resources\\tmp\\tranmger_test");
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
        assert new File("D:\\open_source_projects\\MyDB\\src\\test\\resources\\tmp\\tranmger_test.xid").delete();
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
                    long xid = tmger.begin();
                    transMap.put(xid, (byte)0);
                    transCnt ++;
                    transXID = xid;
                    inTrans = true;
                } else {  // 否则，随机选择提交或回滚当前事务，并更新 transMap 和 inTrans 的状态
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch(status) {
                        case 1:
                            tmger.commit(transXID);
                            break;
                        case 2:
                            tmger.abort(transXID);
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
                            ok = tmger.isActive(xid);
                            break;
                        case 1:
                            ok = tmger.isCommitted(xid);
                            break;
                        case 2:
                            ok = tmger.isAborted(xid);
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
