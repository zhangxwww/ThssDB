package cn.edu.thssdb.parser;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.service.StatementAdapter;
import cn.edu.thssdb.type.ColumnType;

import java.util.List;

public class Visitor extends SQLBaseVisitor {
    private StatementAdapter statementAdapter = new StatementAdapter();

    @Override
    public Object visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {

        String tableName = ctx.table_name().getText();
        List<SQLParser.Column_defContext> rawCols = ctx.column_def();
        Column[] cols = new Column[rawCols.size()];
        String primary = ctx.table_constraint().column_name(0).getText().toUpperCase();
        for (int i = 0; i < rawCols.size(); i++) {
            SQLParser.Column_defContext c = rawCols.get(i);
            List<SQLParser.Column_constraintContext> constraints = c.column_constraint();

            String typeName = c.type_name().getText().toUpperCase();
            String colName = c.column_name().getText();
            ColumnType type;
            int length = -1;
            int isPrimary = 0;
            boolean notNull = false;
            if (constraints.size() == 2 && constraints.get(0).getText().toUpperCase().equals("NOT") &&
                    constraints.get(1).getText().toUpperCase().equals("NULL")) {
                notNull = true;
            }
            if (typeName.startsWith("STRING")) {
                type = ColumnType.STRING;
                length = Integer.parseInt(c.type_name().NUMERIC_LITERAL().getText());
            } else {
                type = ColumnType.valueOf(typeName);
            }
            if (primary.equals(colName)) {
                isPrimary = 1;
            }
            cols[i] = new Column(colName, type, isPrimary, notNull, length);
        }

        statementAdapter.createTable(tableName, cols);

        return null;
    }
}
