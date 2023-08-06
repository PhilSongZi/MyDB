package top.philsongzi.mydb.backend.dm.page;

/**
 * 页面接口
 *
 * @author 小子松
 * @since 2023/8/6
 */
public interface Page {

    // 定义接口方法——上锁、解锁、释放、设置脏页面、判断是否是脏页面、获取页号、获取数据
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
