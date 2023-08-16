package top.philsongzi.mydb.backend.server;

import top.philsongzi.mydb.backend.tbm.TableManager;
import top.philsongzi.mydb.transport.Encoder;
import top.philsongzi.mydb.transport.Packager;
import top.philsongzi.mydb.transport.Transporter;
import top.philsongzi.mydb.transport.Package;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Server 启动一个 ServerSocket 监听端口，当有请求到来时直接把请求丢给一个新线程处理。
 * @author 小子松
 * @since 2023/8/16
 */
public class Server {

    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(
                10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while(true) {
                // 一旦有请求到来，就把请求丢给一个新线程处理
                Socket socket = ss.accept();
                Runnable worker = new HandleSocket(socket, tbm);
                // tpe.execute(new HandleSocket(ss.accept(), tbm));  前面两条合一起
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {}
        }
    }
}


/**
 * HandleSocket 类实现了 Runnable 接口，在建立连接后初始化 Packager，随后就循环接收来自客户端的数据并处理
 */
class HandleSocket implements Runnable {

    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    /**
     * 用于处理客户端请求的 Packager
     */
    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());
        Packager packager = null;
        try {
            Transporter t = new Transporter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t, e);
        } catch(IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }
        Executor exe = new Executor(tbm);
        while(true) {
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch(Exception e) {
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                result = exe.execute(sql);
            } catch (Exception e1) {
                e = e1;
                e.printStackTrace();
            }
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception e1) {
                e1.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
