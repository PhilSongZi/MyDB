package top.philsongzi.mydb.backend.parser.statement;

/**
 * @author 小子松
 * @since 2023/8/3
 */
public class Create {

    // 字段表：表名、字段名列表、字段类型列表、索引
    public String tableName;
    public String[] fieldName;
    public String[] fieldType;
    public String[] index;
}
