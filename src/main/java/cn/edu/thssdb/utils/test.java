package cn.edu.thssdb.utils;
import cn.edu.thssdb.parser.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import org.antlr.runtime.tree.ParseTree;
import org.antlr.v4.runtime.*;
import java.io.PrintStream;

import org.antlr.v4.runtime.CharStream;
import cn.edu.thssdb.server.ThssDB;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.*;



public class test {
    public static void main(String[] args) {

        String len = "INSERT INTO person(id,AGE,NAME) VALUES (32,15,'Bob')";
        CharStream input = CharStreams.fromString(len);
        SQLLexer lexer = new SQLLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        Visitor visitor = new Visitor();
        SQLParser.ParseContext ctxTest = parser.parse();

        System.out.println(visitor.visitParse(ctxTest));

//        SQLBaseListener listener = new SQLBaseListener();

    }
}
