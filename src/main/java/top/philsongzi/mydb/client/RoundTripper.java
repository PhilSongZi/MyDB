package top.philsongzi.mydb.client;

import top.philsongzi.mydb.transport.Packager;
import top.philsongzi.mydb.transport.Package;

/**
 * 实际上实现了单次收发动作。
 * @author 小子松
 * @since 2023/8/16
 */
public class RoundTripper {

    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
