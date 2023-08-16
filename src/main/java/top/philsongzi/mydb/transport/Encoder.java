package top.philsongzi.mydb.transport;

import com.google.common.primitives.Bytes;

import top.philsongzi.mydb.common.Error;

import java.util.Arrays;

/**
 * Encoder 编码 Package，将 Package 编码成 byte[] 数组，或者将 byte[] 数组解码成 Package。
 * 编码和解码的规则：[Flag][data]，flag 为 0 时表示 data 为正常数据，为 1 时表示 data 为错误信息。
 * @author 小子松
 * @since 2023/8/3
 */
public class Encoder {

    public byte[] encode(Package pkg) {
        if(pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if(err.getMessage() != null) {
                msg = err.getMessage();  // 如果有错误信息，就用错误信息
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());  // flag 为 1 表示错误，返回错误信息
        } else {
            return Bytes.concat(new byte[]{0}, pkg.getData());  // flag 为 0 表示正常数据，返回数据
        }
    }

    public Package decode(byte[] data) throws Exception {
        if(data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        if(data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if(data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw Error.InvalidPkgDataException;
        }
    }
}
