package top.philsongzi.mydb.backend.utils;

/**
 * 自定义工具类，用于在基础模块出现错误时强制停机。
 *
 * @author 小子松
 * @since 2023/8/3
 */
public class Panic {

    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
