package top.philsongzi.mydb.backend.dm.pageIndex;

/**
 * 页面信息：包含页面号和空闲空间大小。
 * @author 小子松
 * @since 2023/8/7
 */
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}

