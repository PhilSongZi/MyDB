package top.philsongzi.mydb.backend.dm.page;

import top.philsongzi.mydb.backend.dm.pageCache.PageCache;
import top.philsongzi.mydb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 *
 * @author 小子松
 * @since 2023/8/6
 */
public class PageOne {

    // OF_VC 是有效检查的偏移量，LEN_VC 是有效检查的长度
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    // 启动时设置初始字节
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }
    private static void setVcOpen(byte[] raw) {
        // 用随机字节填充
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    // 关闭时拷贝字节
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }
    private static void setVcClose(byte[] raw) {
        // 将 OF_VC 处的字节拷贝到 OF_VC+LEN_VC 处
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    // 校验字节
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }
    public static boolean checkVc(byte[] raw) {
        // 比较 OF_VC 处的字节和 OF_VC+LEN_VC 处的字节是否相等
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}