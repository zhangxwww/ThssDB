package cn.edu.thssdb.parser;

import cn.edu.thssdb.query.JoinCondition;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.service.StatementAdapter;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;
import jdk.nashorn.internal.ir.TernaryNode;
import org.antlr.v4.runtime.tree.TerminalNode;

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
    public Object visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        statementAdapter.dropTable(tableName);
        return null;
    }

    @Override
    public Object visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        //如果没有colomn name, 说明是插入整行，所以需要获取表的colomns信息
        String tableName = ctx.table_name().getText();
        List<SQLParser.Column_nameContext> colNames = ctx.column_name();
        List<SQLParser.Literal_valueContext> values = ctx.value_entry(0).literal_value();
        Entry[] entries = new Entry[values.size()];
        //转换为Entry[]
        for (int i = 0; i < entries.length; i++) {
            SQLParser.Literal_valueContext val = values.get(i);
            Entry tmp;
            TerminalNode stringVal = val.STRING_LITERAL();
            TerminalNode numericVal = val.NUMERIC_LITERAL();
            if (stringVal == null) {
                tmp = new Entry(Integer.parseInt(numericVal.getText()));
            } else {
                tmp = new Entry(stringVal.getText());
            }
            entries[i] = tmp;
        }

        if (colNames.size() == 0) {
            int attrsNum = statementAdapter.tableAttrsNum(tableName);
            if (values.size() != attrsNum) {
                //TODO 异常处理
                System.out.println("Insert Failure! valueEntries.size()!=attrsNum");
                return null;
            } else {
                statementAdapter.insertTableRow(tableName, new Row(entries));
            }
        } else {
            //如果有colomn name, 那么，value的个数要与colomn个数一致
            if (colNames.size() != values.size()) {
                //TODO 异常处理
                System.out.println("Insert Failure! colNames.size() != values.size()");
            } else {
                String primaryKeyName = statementAdapter.getTablePrimaryAttr(tableName);
                String[] attrNames = new String[colNames.size()];
                boolean hasPrimaryKey = false;
                for (int j = 0; j < attrNames.length; j++) {
                    attrNames[j] = colNames.get(j).getText().toUpperCase();
                    if (attrNames[j].equals(primaryKeyName)){
                        hasPrimaryKey = true;
                    }
                }
                if (hasPrimaryKey){
                    statementAdapter.insertTableRow(attrNames,entries);
                }else{
                    //TODO 异常处理 插入的属性中没有主键
                }
            }
        }

        //且必须含有主键
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

