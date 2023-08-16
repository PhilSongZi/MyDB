package top.philsongzi.mydb.client;

import top.philsongzi.mydb.transport.Packager;
import top.philsongzi.mydb.transport.Package;

/**
 * 客户端。
 * @author 小子松
 * @since 2023/8/3
 */
public class Client {

    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }
}
