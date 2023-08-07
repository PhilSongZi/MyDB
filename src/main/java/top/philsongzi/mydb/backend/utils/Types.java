package top.philsongzi.mydb.backend.utils;

/**
 * @author 小子松
 * @since 2023/8/7
 */
public class Types {
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
