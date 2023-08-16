package top.philsongzi.mydb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * Transporter 类将编码后的数据通过 Socket 进行传输，这里封装了 Socket 的读写操作。
 * @author 小子松
 * @since 2023/8/3
 */
public class Transporter {

    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        // 通过 Socket 获取输入输出流
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public void send(byte[] data) throws Exception {
        // 调用转换方法，将 byte[] 数组转换成十六进制字符串
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    public byte[] receive() throws Exception {
        // 使用 BufferedReader 和 Writer 来直接按行读写
        String line = reader.readLine();
        if(line == null) {
            close();
        }
        return hexDecode(line);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    private String hexEncode(byte[] buf) {
        // 为了避免特殊字符的问题，这里将 byte[] 数组转换成十六进制字符串，并且为信息末尾添加换行符
        return Hex.encodeHexString(buf, true)+"\n";
    }

    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
