package top.philsongzi.mydb.backend.parser.statement;

/**
 * @author 小子松
 * @since 2023/8/3
 */
public class Update {

    public String tableName;
    public String fieldName;
    public String value;
    public Where where;
}
