package top.philsongzi.mydb.backend.common;

/**
 * 共享内存数组，松散的规定数组的可使用范围
 *
 * @author 小子松
 * @since 2023/8/4
 */
public class SubArray {

    // 该数组的原始数据、开始位置、结束位置
    public byte[] raw;
    public int start;
    public int end;

    // 构造函数
    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
