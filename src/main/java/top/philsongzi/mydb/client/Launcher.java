package top.philsongzi.mydb.client;

import top.philsongzi.mydb.transport.Encoder;
import top.philsongzi.mydb.transport.Packager;
import top.philsongzi.mydb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * 客户端的启动入口。
 * @author 小子松
 * @since 2023/8/3
 */
public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
