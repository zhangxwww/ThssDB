package cn.edu.thssdb.utils;
import cn.edu.thssdb.parser.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.service.StatementAdapter;
import cn.edu.thssdb.type.ColumnType;
import org.antlr.runtime.tree.ParseTree;
import org.antlr.v4.runtime.*;

import java.awt.*;
import java.io.PrintStream;

import org.antlr.v4.runtime.CharStream;
import cn.edu.thssdb.server.ThssDB;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.util.*;
import java.util.List;


public class test {
    private static void showTable(List<String> columns, List<List<String>> rows) {
        JFrame window = new JFrame("Result");
        window.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        JPanel panel = new JPanel(new BorderLayout());
        JTable table = new JTable();
        Vector<String> cols = new Vector<>();
        for (String c : columns) {
            cols.add(c);
        }
        DefaultTableModel tm = new DefaultTableModel(null, (Vector) cols);
        Object[] cs = columns.toArray();
        tm.setColumnCount(3);
        tm.addRow(cs);
        for (List<String> row : rows) {
            tm.addRow(row.toArray());
        }
        table.setModel(tm);
        panel.add(table, BorderLayout.CENTER);
        window.setContentPane(panel);
        window.pack();
        window.setLocationRelativeTo(null);
        window.setVisible(true);
    }

    public static void main(String[] args) {
        List<String> columnList = new ArrayList<String>();
        columnList.add("ID");
        columnList.add("NAME");
        columnList.add("AGE");

        List<List<String>> rowList = new ArrayList<>(0);
        showTable(columnList,rowList);

//        String len = "insert into student(name) VALUES ('bob',15)";
//        CharStream input = CharStreams.fromString(len);
//        SQLLexer lexer = new SQLLexer(input);
//        CommonTokenStream tokens = new CommonTokenStream(lexer);
//        SQLParser parser = new SQLParser(tokens);
//        StatementAdapter adapter = new StatementAdapter();
//         Visitor visitor = new Visitor(adapter);
//        SQLParser.ParseContext ctxTest = parser.parse();
////
//        System.out.println(visitor.visitParse(ctxTest));

//        SQLBaseListener listener = new SQLBaseListener();

    }
}
