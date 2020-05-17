package cn.edu.thssdb.parser;

import cn.edu.thssdb.query.JoinCondition;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.service.StatementAdapter;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.util.ArrayList;
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

    @Override
    public Object visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        // select clause
        List<SQLParser.Result_columnContext> cols = ctx.result_column();
        int m = cols.size();
        List <Pair<String, String>> results = new ArrayList<Pair<String, String>>(m);
        for (int i = 0; i < m; i++) {
            SQLParser.Column_full_nameContext c = cols.get(i).column_full_name();
            SQLParser.Table_nameContext tableNameContext = c.table_name();
            String tableName = "";
            if (tableNameContext != null) {
                tableName = tableNameContext.getText();
            }
            String attrName = c.column_name().getText();
            results.set(i, new Pair<String, String>(tableName, attrName));
        }

        // from clause
        List<SQLParser.Table_queryContext> tables = ctx.table_query();
        boolean hasJoin = false;
        String table1 = "";
        String table2 = "";
        int numTables = tables.size();
        if (numTables == 1) {
            table1 = tables.get(0).table_name().toString();
        } else {
            // TODO: Exception Handle
            table1 = tables.get(0).table_name().toString();
            table2 = tables.get(1).table_name().toString();
            hasJoin = true;
            JoinCondition joinCondition = (JoinCondition) visitMultiple_condition(tables.get(2).multiple_condition());
        }
        return null;
    }

    @Override
    public Object visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        return null;
    }
}
