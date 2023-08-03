package top.philsongzi.mydb.common;

/**
 * 自定义错误类，集合了所有模块中可能出现的错误。
 *
 * @author 小子松
 * @since 2023/8/3
 */
public class Error {

    // common：通用错误——满缓存、文件已存在、文件不存在、文件无法读写
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write!");

    // dm

    // tm:Transaction Manager 中的错误
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

    // vm

    // tbm

    // parser

    // transport

    // server

    // launcher
}
