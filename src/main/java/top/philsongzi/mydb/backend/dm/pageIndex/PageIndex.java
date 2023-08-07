package top.philsongzi.mydb.backend.dm.pageIndex;

import top.philsongzi.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面索引类
 *
 * @author 小子松
 * @since 2023/8/7
 */
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // 怎么分？—— 页大小（页面缓存接口中定义的属性） / 区间数（前面定好的 40 页）
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    // 用于同步
    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    // 在上层模块使用完之后，将 PageInfo 插入到 PageIndex 中
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    // 从 PageIndex 中获取页面 —— 选择一个空间大小大于等于 spaceSize 的 PageInfo
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) {number ++;}
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
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
