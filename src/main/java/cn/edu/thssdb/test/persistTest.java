package cn.edu.thssdb.test;

import cn.edu.thssdb.parser.SQLBaseVisitor;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.persist.NaiveSerializationPersist;
import cn.edu.thssdb.persist.PageFilePersist;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.*;
import java.util.ArrayList;

class Person implements Serializable {
    //手动定义序列号
    private static final long serialVersionUID=1l;
    private String name;
    private int age;

    //根据JavaBean规范定义各种方法.
    //不过由于这里不需要equals与hashCode方法,所以没有定义.
    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age){
        this.age=age;
    }

    public String toString(){
        return "["+name+","+age+"]";
    }
}

public class persistTest {
    private void test0(){
        try {
            //写入对象,即序列化过程
            ByteArrayOutputStream bOutStream = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bOutStream);
            oos.writeObject(new Person("A", 21));
            oos.writeObject(new Person("B", 19));
            oos.writeObject(new Person("C", 23));
            oos.writeObject(new Person("D", 20));
            byte[] bData = bOutStream.toByteArray();
            //读出对象,即反序列化过程.
            ByteArrayInputStream bInStream = new ByteArrayInputStream(bData);
            ObjectInputStream ois = new ObjectInputStream(bInStream);
            Object o1 = ois.readObject();
            System.out.println("读出第一个对象:" + o1);
            Object o2 = ois.readObject();
            System.out.println("读出第二个对象:" + o2);
            Object o3 = ois.readObject();
            System.out.println("读出第三个对象:" + o3);
            Object o4 = ois.readObject();
            System.out.println("读出第四个对象:" + o4);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //朴素序列化的测试
    private static void test1(){
        try {
            //写入对象,即序列化过程
            ByteArrayOutputStream bOutStream = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bOutStream);
            oos.writeObject(new Person("A", 21));
            oos.writeObject(new Person("B", 19));
            oos.writeObject(new Person("C", 23));
            oos.writeObject(new Person("D", 20));
            byte[] bData = bOutStream.toByteArray();


            NaiveSerializationPersist nsp = new NaiveSerializationPersist();
            nsp.storeTable("person",bData);


            //读出对象,即反序列化过程.
            byte[] bNewData = nsp.retrieveTable("person");

            ByteArrayInputStream bInStream = new ByteArrayInputStream(bNewData);
            ObjectInputStream ois = new ObjectInputStream(bInStream);
            Object o1 = ois.readObject();
            System.out.println("读出第一个对象:" + o1);
            Object o2 = ois.readObject();
            System.out.println("读出第二个对象:" + o2);
            Object o3 = ois.readObject();
            System.out.println("读出第三个对象:" + o3);
            Object o4 = ois.readObject();
            System.out.println("读出第四个对象:" + o4);
            Object o5 = ois.readObject();
            System.out.println("读出第四个对象:" + o5);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //分页存储的测试1-验证可以保存记录到磁盘文件和从磁盘文件读回记录
    private static void test2(){
        try {
            PageFilePersist persistManager;//暂时放在这里，理应属于一个database
            persistManager = new PageFilePersist(20);//暂时放在这里，理应属于一个database

            int nCol = 3;
            int nRow = 100;
            Row[] rows = new Row[nRow];
            Entry[] entries = new Entry[nCol];
            for (int i = 0; i < nRow; i++){
                entries[0] = new Entry(i);
                entries[1] = new Entry("a"+i);
                entries[2] = new Entry("heng"+i);
                rows[i] = new Row(entries);
            }
            Column[] cols = new Column[nCol];
            cols[0] = new Column("ID",ColumnType.INT,1,true,4);
            cols[1] = new Column("name",ColumnType.STRING,0,true,8);
            cols[2] = new Column("sig",ColumnType.STRING,0,true,8);

            Table testTable = new Table("Test","test",cols,"ID");
//            for (Row r : rows){
//                testTable.insert(r);
//            }
            //下面两条用于persist.test2,理应从database信息里获取的，不是自己手写
//            persistManager.tableFramesNum.put("test",0);
//            persistManager.tablePageMap.put("test",new ArrayList<>(0));
//            testTable.serialize(persistManager);
//            testTable.persist(persistManager);
            //到这一步就能看到文件下对应的test0-6文件

            //把上面初始化table语句下开始注释掉，那么理应能从test0-6读回rows
            //下面这句用于理应从database信息里获取的，不是自己手写
            persistManager.tableFramesNum.put("test",7);
            persistManager.tablePageMap.put("test",new ArrayList<>(0));
            ArrayList<Row> recoverRows = testTable.deserialize(persistManager);
            testTable.printRowList(recoverRows);
            System.out.println(persistManager.tableFramesNum.get("test"));
            System.out.println(persistManager.tablePageMap.get("test"));
            //确实如此，读到100条记录

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //分页存储的测试2-验证页面置换
    private static void test3(){
        try {
            PageFilePersist persistManager;//暂时放在这里，理应属于一个database
            persistManager = new PageFilePersist(10);//暂时放在这里，理应属于一个database

            int nCol = 3;
            Column[] cols = new Column[nCol];
            cols[0] = new Column("ID",ColumnType.INT,1,true,4);
            cols[1] = new Column("name",ColumnType.STRING,0,true,8);
            cols[2] = new Column("sig",ColumnType.STRING,0,true,8);

            Table testTable = new Table("Test","test",cols,"ID");
            //下面这句用于理应从database信息里获取的，不是自己手写
            persistManager.tableFramesNum.put("test",7);
            persistManager.tablePageMap.put("test",new ArrayList<>(0));
            persistManager.setTablePageMap();
            ArrayList<Row> recoverRows = testTable.deserialize(persistManager);
            //到这里，test的7页数据已经在bufferPool中了
//            testTable.printRowList(recoverRows);
//            System.out.println(persistManager.tableFramesNum.get("test"));
//            System.out.println(persistManager.tablePageMap.get("test"));


            nCol = 3;
            int nRow = 100;
            Row[] rows = new Row[nRow];
            Entry[] entries = new Entry[nCol];
            for (int i = 0; i < nRow; i++){
                entries[0] = new Entry(i);
                entries[1] = new Entry("b"+i);
                entries[2] = new Entry("heng"+i);
                rows[i] = new Row(entries);
            }
            cols = new Column[nCol];
            cols[0] = new Column("ID",ColumnType.INT,1,true,4);
            cols[1] = new Column("name",ColumnType.STRING,0,true,8);
            cols[2] = new Column("sig",ColumnType.STRING,0,true,8);

            Table testTable2 = new Table("Test","test2",cols,"ID");
            for (Row r : rows){
                testTable2.insert(r);
            }
            //下面两条用于persist.test2,理应从database信息里获取的，不是自己手写
            persistManager.tableFramesNum.put("test2",0);
            persistManager.tablePageMap.put("test2",new ArrayList<>(0));
            persistManager.setTablePageMap();
            testTable2.serialize(persistManager);
            System.out.println("test:"+persistManager.tableFramesNum.get("test"));
            System.out.println(persistManager.tablePageMap.get("test"));
            System.out.println("test2:"+persistManager.tableFramesNum.get("test2"));
            System.out.println(persistManager.tablePageMap.get("test2"));
            //理应看到test表帧数7，bufferpool页面被置换出去4页，只有3页
            //同时观察文件夹下被替换出去的frame对应的文件，修改时间已变。
            //test2表帧数7，bufferpool中占7页

            testTable2.persist(persistManager);
            //到这一步就能看到文件下对应的test0-6文件




        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
//        test3();

    }
}
