package top.philsongzi.mydb.backend.tm;

import top.philsongzi.mydb.backend.utils.Panic;
import top.philsongzi.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * TM 通过维护 XID 文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态。
 *
 * @author 小子松
 * @since 2023/8/3
 */
public interface TransactionManager {

    // 事务的操作方法：开始、提交、取消、查询状态
    long begin();
    void commit(long xid);
    void abort(long xid);

    // 查询一个事务的状态：进行中、提交、取消
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);

    // 关闭TM
    void close();

    // 创建一个TM实例——单例模式，通过create()或open()方法创建
    static TransactionManagerImpl create(String path) {
        /**
         * 创建一个新的XID文件
         */
        // 路径+文件名后缀
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!file.createNewFile()) {
                // 如果文件已存在，则报错
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 如果文件不可读或不可写，则报错
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            // 从零开始XID文件时需要先写入一个空的XID文件头，即设置xidCounter为0，否则后续校验时不合法
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fileChannel);
    }

    static TransactionManagerImpl open(String path) {
        /**
         * 打开一个已存在的XID文件
         */
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        // 照旧是文件存在与否与可读可写的判断
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel channel = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            channel = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, channel);
    }
}
