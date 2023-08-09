package top.philsongzi.mydb.backend.vm;

import top.philsongzi.mydb.backend.tm.TransactionManager;

/**
 * 版本可见性管理
 *
 * @author 小子松
 * @since 2023/8/9
 */
public class Visibility {

    // 取出要修改的数据X的最新提交版本，检查该最新版本的创建者是否对当前事务可见
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    // 读提交隔离级别下，判断某个记录对事务 t 是否可见
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) {return true;}

        if(tm.isCommitted(xmin)) {
            if(xmax == 0) {return true;}
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    // 可重复读隔离级别下，一个版本是否对事务可见的判断
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) {return true;}

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) {return true;}
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
