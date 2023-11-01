package top.philsongzi.mydb.backend.dm.pageIndex;

import top.philsongzi.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面索引类：缓存了每一页的空闲空间。用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
 * 实现方式：
 * 将一页的空间划分成了 40 个区间。
 * 在启动时，就会遍历所有的页面信息，获取页面的空闲空间，安排到这 40 个区间中。
 * insert 在请求一个页时，会首先将所需的空间向上取整，映射到某一个区间，随后取出这个区间的任何一页，都可以满足需求。
 * @author 小子松
 * @since 2023/8/7
 */
public class PageIndex {
    // 将一页划成40个区间。怎么分？—— 页大小（页面缓存接口中定义的属性） / 区间数（前面定好的 40 页）
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    // 40个区间，每个区间都是一个 List，用于存放空闲空间大小在这个区间的页面
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 在上层模块使用完之后，将 PageInfo 插入到 PageIndex 中
     * @param pgno 页面号
     * @param freeSpace 空闲空间大小
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从 PageIndex 中获取页面 —— 选择一个空间大小大于等于 spaceSize 的 PageInfo
     * @param spaceSize 所需的空间大小
     * @return PageInfo
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) {number ++;}
            while(number <= INTERVALS_NO) {
                if(lists[number].isEmpty()) {
                    number ++;
                    continue;
                }
                // 返回的 PageInfo 中包含了页面号和空闲空间大小
                // 同时，被选中的页会直接从 PageIndex 中移除，意味着，同一个页面是不允许并发写的！
                // 上层使用完这个页面之后，需要将它重新插入到 PageIndex
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
