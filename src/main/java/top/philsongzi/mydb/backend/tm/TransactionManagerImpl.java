package top.philsongzi.mydb.backend.tm;

import top.philsongzi.mydb.backend.utils.Panic;
import top.philsongzi.mydb.backend.utils.Parser;
import top.philsongzi.mydb.common.Error;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TM 的实例类。TM 的实例类是单例的，通过 create() 或 open() 方法创建。
 *
 * @author 小子松
 * @since 2023/8/3
 */
public class TransactionManagerImpl implements TransactionManager{

    /**
     * 在 MYDB 中，每一个事务都有一个 XID，这个 ID 唯一标识了这个事务。
     * 事务的 XID 从 1 开始标号，并自增，不可重复。
     * 并特殊规定 XID 0 是一个超级事务（Super Transaction）。
     * 当一些操作想在没有申请事务的情况下进行，那么可以将操作的 XID 设置为 0。
     * XID 为 0 的事务的状态永远是 committed。
     */
    // XID 文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;
    // 事务的三种状态: 事务正在进行、事务已提交、事务已取消
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;
    // 超级事务，永远为 committed 状态
    public static final long SUPER_XID = 0;
    // XID 文件后缀
    static final String XID_SUFFIX = ".xid";

    // 读写方式采用NIO的FileChannel
    private RandomAccessFile file;
    private FileChannel fileChannel;
    private long xidCounter;
    private Lock counterLock;

    // 构造函数
    TransactionManagerImpl(RandomAccessFile raf, FileChannel fileChannel) {
        this.file = raf;
        this.fileChannel = fileChannel;
        counterLock = new ReentrantLock();
        // 检查 XID 文件是否合法
        checkXIDCounter();
    }

    private void checkXIDCounter() {

        /**
         * 检查 XID 文件是否合法
         * 读取 XID_FILE_HEADER 中的 xidcounter，根据它计算文件的理论长度，对比实际长度
         * 通过文件头的 8 字节数字反推文件的理论长度，与文件的实际长度做对比。如果不同则认为 XID 文件不合法。
         */
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            // 将读取位置设为0，然后读取8个字节到buf中.
            fileChannel.position(0);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    private long getXidPosition(long xid) {
        /**
         * 根据事务 ID 计算出该事务在 XID 文件中的位置
         */
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    private void updateXID(long xid, byte status) {
        /**
         * 更新 xid 事务状态
         * @param xid 事务 ID
         * @param status 事务状态：FIELD_TRAN_ACTIVE、FIELD_TRAN_COMMITTED、FIELD_TRAN_ABORTED
         */
        long position = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        buf.put(status);
        buf.flip();
        try {
            fileChannel.position(position);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 强制刷入到文件中:FileChannel 的 force() 方法，强制同步缓存内容到文件中，类似于 BIO 中的 flush() 方法
        // force 方法的参数是一个布尔，表示是否同步文件的元数据（例如最后修改时间等）。这里我们不需要同步元数据，所以传入 false。
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 递增xid计数器，并更新XID Header
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 强制刷入到文件中:FileChannel 的 force() 方法，强制同步缓存内容到文件中，类似于 BIO 中的 flush() 方法
        // force 方法的参数是一个布尔，表示是否同步文件的元数据（例如最后修改时间等）。这里我们不需要同步元数据，所以传入 false。
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 开启一个新事务，返回事务 ID
    @Override
    public long begin() {
        // 事务 ID 从 1 开始，0 为超级事务
        counterLock.lock();
        try {
            // 首先设置 xidCounter+1 事务的状态为 committed
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            // 然后递增 xidCounter，并更新头文件
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // 提交一个XID事务，借助updateXID()方法实现
    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 取消一个XID事务 借助updateXID()方法实现
    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // 1.是否正在进行
    @Override
    public boolean isActive(long xid) {
        // 检查之前，先判断是否是超级事务
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    // 2.是否是已提交
    @Override
    public boolean isCommitted(long xid) {
        // 检查之前，先判断是否是超级事务
        if (xid == SUPER_XID) {
            return true;
        }
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 3.是否是已取消
    @Override
    public boolean isAborted(long xid) {
        // 检查之前，先判断是否是超级事务
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    // 关闭TM
    @Override
    public void close() {
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
