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

    // dm: data manager 中的 Exception —— 错误日志、内存过小、数据过大、数据库繁忙
    public static final Exception BadLogFileException = new RuntimeException("Bad log file!");
    public static final Exception MemTooSmallException = new RuntimeException("Memory too small!");
    public static final Exception DataTooLargeException = new RuntimeException("Data too large!");
    public static final Exception DatabaseBusyException = new RuntimeException("Database is busy!");

    // tm:Transaction Manager 中的异常——XID文件问题。
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

    // vm: Version Manager 中异常类型——死锁、并发更新、空条目
    public static final Exception DeadlockException = new RuntimeException("Deadlock!");
    public static final Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");
    public static final Exception NullEntryException = new RuntimeException("Null entry!");

    // tbm 的异常：非法字段名、字段不存在、字段未索引、逻辑不合法操作、值不合法、重复表、表未找到
    public static final Exception InvalidFieldException = new RuntimeException("Invalid field type!");
    public static final Exception FieldNotFoundException = new RuntimeException("Field not found!");
    public static final Exception FieldNotIndexedException = new RuntimeException("Field not indexed!");
    public static final Exception InvalidLogOpException = new RuntimeException("Invalid logic operation!");
    public static final Exception InvalidValuesException = new RuntimeException("Invalid values!");
    public static final Exception DuplicatedTableException = new RuntimeException("Duplicated table!");
    public static final Exception TableNotFoundException = new RuntimeException("Table not found!");

    // parser 的异常：非法命令、表不含索引
    public static final Exception InvalidCommandException = new RuntimeException("Invalid command!");
    public static final Exception TableNoIndexException = new RuntimeException("Table has no index!");

    // transport
    public static final Exception InvalidPkgDataException = new RuntimeException("Invalid package data!");

    // server

    // launcher
}
