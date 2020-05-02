package cn.edu.thssdb.utils;
import cn.edu.thssdb.parser.*;
import org.antlr.runtime.tree.ParseTree;
import org.antlr.v4.runtime.*;
import java.io.PrintStream;

import org.antlr.v4.runtime.CharStream;
import cn.edu.thssdb.server.ThssDB;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.*;



public class test {
    public static void main(String[] args) {
        String len = "select name from student where id = 1001;\r\n";
        CharStream input = CharStreams.fromString(len);
        SQLLexer lexer = new SQLLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        SQLBaseVisitor visitor = new SQLBaseVisitor();
        SQLParser.ParseContext ctxTest = parser.parse();

//        System.out.println(visitor.visitParse(ctxTest));

//        SQLBaseListener listener = new SQLBaseListener();

    }
}
