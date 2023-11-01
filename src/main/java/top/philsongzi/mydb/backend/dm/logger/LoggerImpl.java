package top.philsongzi.mydb.backend.dm.logger;

import com.google.common.primitives.Bytes;

import top.philsongzi.mydb.backend.utils.Panic;
import top.philsongzi.mydb.backend.utils.Parser;
import top.philsongzi.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件的实现：[XChecksum][Log1][Log2][Log3]...[LogN][BadTail]。
 * XChecksum: 日志文件的校验和；
 * Log: 日志；
 * BadTail: 日志文件的尾部，可能是不完整的日志，需要移除（不一定存在）。
 * 单条日志的内容：[Size][Checksum][Data]
 * 其中，Size 是一个四字节整数，标识了 Data 段的字节数。Checksum 则是该条日志的校验和。
 * @author 小子松
 * @since 2023/8/7
 */
public class LoggerImpl implements Logger {

    // 种子
    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /**
     * 初始化日志文件
     */
    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    /**
     * 检查并移除bad tail，由于 BadTail 该条日志尚未写入完成，文件的校验和也就不会包含该日志的校验和，去掉 BadTail 即可保证日志文件的一致性。
     */
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if(log == null) {break;}
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            // 截断日志文件到最后一个完整的日志
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    /**
     * 单条日志的校验和（checkSum）计算：通过一个指定的种子实现
     * @param xCheck 校验和
     * @param log 日志
     * @return 校验和
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 写入日志文件：包裹成日志格式，添加日志的大小和校验和
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            // 定位到日志文件的末尾、追加日志
            fc.position(fc.size());
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        // 更新日志文件的校验和
        updateXChecksum(log);
    }

    /**
     * 更新日志文件的校验和
     * @param log 日志
     */
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 包装日志：添加日志的大小和校验和
     * @param data 日志
     * @return 包装后的日志
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    /**
     * 截断日志文件，主要用来去除 bad tail
     * @param x 截断的位置
     * @throws Exception 异常
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * next() 方法的内部实现
     * @return 日志
     */
    private byte[] internNext() {
        if(position + OF_DATA >= fileSize) {
            return null;
        }

        // 读取日志的大小
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if(position + size + OF_DATA > fileSize) {
            return null;
        }

         // 读取 checkSum 和 data
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }

        // 校验 checkSum
        byte[] log = buf.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if(checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    /**
     * 读取日志文件中的下一条日志：迭代器模式，使用 next() 读取，内部实现依靠 internNext() 方法
     * @return 日志
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) {return null;}
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 回溯到日志文件的开头
     */
    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

}

