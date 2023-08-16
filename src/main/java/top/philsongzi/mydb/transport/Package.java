package top.philsongzi.mydb.transport;

/**
 * 通信传输的最基本结构 Package。每个 Package 在发送前都会经由 Encoder 被封装成一个 byte[] 数组。
 * @author 小子松
 * @since 2023/8/3
 */
public class Package {

    // 数据、错误类型
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
