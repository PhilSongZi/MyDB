package top.philsongzi.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.philsongzi.mydb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存：
 * 不采用LRU——LRU驱逐资源不可控，上层无法感知驱逐的是哪个资源。引用计数，只有在上层模块助动释放引用，缓存确保没有模块使用此资源时才驱逐。
 * 当缓存满，引用计数无法自动释放缓存，直接报错（类似JVM）OOM。
 * @author 小子松
 * @since 2023/8/4
 */
public abstract class AbstractCache<T> {

    // 引用计数，除了普通的缓存功能，还需要另外维护一个计数。
    // 除此以外，为了应对多线程场景，还需要记录哪些资源正在从数据源获取中（从数据源获取资源是一个相对费时的操作）。
    // 因此，有下面三个 HashMap。
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在被获取的资源

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    // 构造函数
    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }


    /**
     * get 方法 获取资源
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        // 1.在通过 get() 方法获取资源时，首先进入一个死循环，来无限尝试从缓存里获取。
        while(true) {
            lock.lock();
            // 1.1.首先就需要检查这个时候是否有其他线程正在从数据源获取这个资源，如果有，就过会再来看看
            if(getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // 1.2.如果没有其他线程在获取这个资源，那么就可以尝试从缓存中获取了
            if(cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                // 记得给资源的引用计数加一
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 1.3.尝试获取该资源
            // a.判断缓存是否已满，如果已满，就抛出一个异常
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            // b.如果缓存未满，就在 getting 中注册一下，该线程准备从数据源获取资源了
            count ++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        // 2.从数据源获取资源
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            // 如果获取失败，就把 getting 中的注册信息清除掉
            lock.lock();
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);       // 获取完成要从 getting 中清除注册信息
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * release 方法 强行释放缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            // 释放一个缓存时，直接从 references 中减 1，如果已经减到 0 了，就可以回源，并且删除缓存中所有相关的结构
            int ref = references.get(key) - 1;
            if(ref == 0) {
                T obj = cache.get(key);
                // 调用抽象方法释放缓存
                releaseForCache(obj);
                // 删除缓存中所有相关的结构
                references.remove(key);
                cache.remove(key);
                count --;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * close 方法 关闭缓存,写回所有资源
     */
    protected void close() {
        // 缓存应当还有以一个安全关闭的功能，在关闭时，需要将缓存中所有的资源强行回源。
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
