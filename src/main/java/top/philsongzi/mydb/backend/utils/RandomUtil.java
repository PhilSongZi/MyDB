package top.philsongzi.mydb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * 随机工具类，用于生成填充第一页的随机字节
 *
 * @author 小子松
 * @since 2023/8/6
 */
public class RandomUtil {

    public static byte[] randomBytes(int length) {
        Random random = new SecureRandom();
        byte[] buf = new byte[length];
        random.nextBytes(buf);
        return buf;
    }
}
