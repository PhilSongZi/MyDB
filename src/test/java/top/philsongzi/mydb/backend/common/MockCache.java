package top.philsongzi.mydb.backend.common;

/**
 * @author 小子松
 * @since 2023/8/4
 */
public class MockCache extends AbstractCache<Long> {

    public MockCache() {
        super(50);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {}

}