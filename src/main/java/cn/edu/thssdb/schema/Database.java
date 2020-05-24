package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import cn.edu.thssdb.persist.PageFilePersist;
import cn.edu.thssdb.utils.Global;

class tableInfo implements Serializable {
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    public int primaryIndex;
    public int frameNum;

    public tableInfo(String dbName, String tbName, ArrayList<Column> cols, int primaryId, int frameN) {
        databaseName = dbName;
        tableName = tbName;
        columns = cols;
        primaryIndex = primaryId;
        frameNum = frameN;
    }
}


public class Database {

    private String name;
    private HashMap<String, Table> tables;
    ReentrantReadWriteLock lock;

    private ArrayList<tableInfo> tableInfos;
    public PageFilePersist persistManager;


    public Database(String name) {
        this.name = name;
        this.tables = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();

    }

    public void persist() {
        // TODO
        //把tables先存好
        for (Table t : tables.values()) {
            byte[] bData = t.serialize();
            persistManager.storeTable(t.tableName, bData); //to buffer pool
            persistManager.flushTable(t.tableName); // to disc
        }
        //把管理信息存好
        //根据tables和psm来set一下tableInfos
        tableInfos.clear();
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(Global.ROOT_PATH + name + "/manage", false));
            HashMap<String, Integer> tableFramesNum = persistManager.tableFramesNum;
            for (Table t : tables.values()) {
                int frameNum = tableFramesNum.get(t.tableName);
                tableInfo tbInfo = new tableInfo(name, t.tableName, t.getColumns(), t.primaryIndex, frameNum);
                objectOutputStream.writeObject(tbInfo);
                tableInfos.add(tbInfo);
            }
            objectOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //新建表
    public Table createTable(String tbName, Column[] columns) {
        // TODO
        if (tables.containsKey(tbName)) {
            System.out.println("Table " + tbName + " exists");
        } else {
            Table newTable = new Table(name, tbName, columns);
            tables.put(tbName, newTable);
            persistManager.addTable(tbName); //在存储管理中更新信息
            return newTable;
        }
        return null;

    }

    //丢弃整张表
    public void dropTable(String tbName) {
        // TODO
        try {
            if (!tables.containsKey(tbName)) {
                System.out.println("Table " + tbName + " not exists");
            } else {
                //把磁盘文件删除
                int frameNum = persistManager.tableFramesNum.get(tbName);
                //注意：psm中tableFramesNum存储的是：上一次table进行serialize时占据多少frame
                //也就是说，如果table有过增删，但未曾serialize的化，frameNum记录的是旧值
                //且由于serialize过后不一定进行了persist，所以不见得所有的frame都存到了磁盘中
                for (int i = 0; i < frameNum; i++) {
                    File delFile = new File(Global.ROOT_PATH + name + "/" + tbName + i);
                    if (delFile.exists()) {
                        boolean succ = delFile.delete();
                        if (!succ) {
                            throw new IOException("Deleting table " + tbName + " failed.");
                        }
                    }
                }
                tables.remove(tbName);
                //在存储管理中更新信息
                persistManager.deleteTable(tbName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    //丢弃自身
    public void drop() {
        // TODO
        // 每个database有自己的内存池，由java回收
        // 从磁盘中删去文件
        try {
            File dir = new File(Global.ROOT_PATH + name);
            if (dir.isDirectory()) {
                String[] children = dir.list();
                for (int i = 0; i < children.length; i++) {
                    boolean success = (new File(dir, children[i])).delete();
                    if (!success)
                        throw new IOException("Deleting database failed.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void recover() {
        // 从manage文件中恢复管理信息
        tableInfos = new ArrayList<>(0);
        tables.clear();
        HashMap<String, Integer> tableFramesNum = new HashMap<>();
        try {
            File manageFile = new File(Global.ROOT_PATH + name + "/manage");
            if (!manageFile.exists()) {
                manageFile.createNewFile();
            } else {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(Global.ROOT_PATH + name + "/manage"));
                while (true) {
                    tableInfo tbInfo = (tableInfo) ois.readObject();
                    Column[] cols = new Column[tbInfo.columns.size()];
                    for (int i = 0; i < cols.length; i++) {
                        cols[i] = tbInfo.columns.get(i);
                    }
                    Table newTable = new Table(name, tbInfo.tableName, cols, tbInfo.primaryIndex);
                    tables.put(tbInfo.tableName, newTable);
                    tableFramesNum.put(tbInfo.tableName, tbInfo.frameNum);
                    tableInfos.add(tbInfo);
                }
            }
        } catch (EOFException e) {
            System.out.println("读取数据库的表信息： 类对象已完全读入");
        } catch (FileNotFoundException e) {

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            persistManager = new PageFilePersist(name, Global.BUFFER_POOL_PAGE_NUM, tableFramesNum);
            System.out.println("recover database: " + name);
            System.out.println("tableName & frameNum: ");
            for (String key : tableFramesNum.keySet()) {
                System.out.println(key + " Count:" + tableFramesNum.get(key));
            }
            System.out.println("tableInfo: ");
            for (tableInfo tb : tableInfos) {
                System.out.println(tb.tableName + tb.columns.toString());
            }
            for (Table t : tables.values()) {
                byte[] bData = persistManager.retrieveTable(t.tableName);
                t.recover(bData); //表获取数据
            }
        }
    }

    public String getName() {
        return name;
    }

    public void quit() {
        //存储到磁盘
        persist();
        //回收内存池
        persistManager = null;
    }

    public Table getTable(String tbName) {
        return tables.get(tbName);
    }
}
