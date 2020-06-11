package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.query.WhereCondition;
import cn.edu.thssdb.server.ThssDB;

import java.io.*;
import java.nio.file.DirectoryNotEmptyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Global.*;

public class Manager {
    private HashMap<String, Database> databases;
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    private String curDatabase;

    public Manager() {
        // TODO
        curDatabase = null;
        databases = new HashMap<>(0);
        //从文件manage.txt中恢复
        try {
            File dir = new File(Global.ROOT_PATH);
            if (!dir.exists()) {
                dir.mkdir();
            }


            File file = new File(Global.ROOT_PATH + "manage.txt");
            if (!file.exists()) {
                boolean success = file.createNewFile();
                if (!success) {
                    throw new IOException("Creating manage.txt failed.");
                }
            } else {
                FileReader fr = new FileReader(Global.ROOT_PATH + "manage.txt");
                BufferedReader br = new BufferedReader(fr);
                String databaseName = "";
                String[] arrs = null;
                while ((databaseName = br.readLine()) != null) {
                    if (databaseName.length() > 0) {
                        Database tmpDB = new Database(databaseName);
                        databases.put(databaseName, tmpDB);
                        System.out.println("Manager has databases: " + databaseName);
                    }
                }
                br.close();
                fr.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createDatabaseIfNotExists(String dbName) {
        if (databases.containsKey(dbName)) {
            System.out.println("Manager.java:createDatabaseIfNotExists: Database " + dbName + " exists.");
        } else {
            try {
                Database newDatabase = new Database(dbName);
                databases.put(dbName, newDatabase);
                File file = new File(Global.ROOT_PATH + "manage.txt");
                FileOutputStream fos = new FileOutputStream(file, true);
                String content = dbName + "\r\n";
                fos.write(content.getBytes());
                //create its corresponding directory
                File dir = new File(Global.ROOT_PATH + dbName);
                if (!dir.exists()) {
                    dir.mkdir();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public void deleteDatabase(String dbName) {
        if (!databases.containsKey(dbName)) {
            System.out.println("Manager.java:cdeleteDatabase: Database " + dbName + " not exists.");
        } else {
            Database delDatabase = databases.get(dbName);
            delDatabase.drop();
            databases.remove(dbName);
            //delete the empty directory
            String dirPath = Global.ROOT_PATH + dbName;
            boolean success = (new File(dirPath)).delete();
            if (success) {
                System.out.println("Successfully deleted empty directory: " + dirPath);
            } else {
                System.out.println("Failed to delete empty directory: " + dirPath);
            }
            // delete info in manage.txt
            try {
                StringBuilder remainDatabases = new StringBuilder();
                File file = new File(Global.ROOT_PATH + "manage.txt");
                if (file.exists()) {
                    FileReader fr = new FileReader(file);
                    BufferedReader br = new BufferedReader(fr);
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        if (line.length() > 0) {
                            System.out.println(line);
                            if (!line.toUpperCase().trim().equals(dbName.toUpperCase())){
                                remainDatabases.append(line).append("\r\n");
                            }
                        }
                    }
                    br.close();
                    fr.close();

                    //清空该文件
                    FileOutputStream fileWriter = new FileOutputStream(file, false);
                    fileWriter.write(remainDatabases.toString().getBytes());
                    fileWriter.flush();
                    fileWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public Database switchDatabase(String dbName) {
        if (curDatabase != null) {
            Database lastDB = databases.get(curDatabase);
            lastDB.quit(); //存储到磁盘且回收内存池
            System.out.println("Last database: " + curDatabase);
        }
        if (!databases.containsKey(dbName)) {
            System.out.println("Manager.java:switchDatabase: Database " + dbName + " not exists.");
        } else {
            Database curDB = databases.get(dbName);
            curDatabase = dbName;
            try {
                curDB.recover();
            } catch (IOException e) {
                System.out.println("Recover database: " + dbName + " failed.");
                e.printStackTrace();
            }
            System.out.println("Switch to database: " + dbName);
            return curDB;
        }
        return null;
    }

    private Database getCurDatabase() {
        return databases.get(curDatabase);
    }

    private static class ManagerHolder {
        private static final Manager INSTANCE = new Manager();

        private ManagerHolder() {

        }
    }
}
