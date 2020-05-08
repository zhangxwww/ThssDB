package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

import java.io.*;
import java.nio.file.DirectoryNotEmptyException;
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
    try{
      File file = new File(Global.ROOT_PATH+"manage.txt");
      if(!file.exists()) {
        boolean success = file.createNewFile();
        if (!success){
          throw new IOException("Creating manage.txt failed.");
        }
      }else{
        FileReader fr=new FileReader(Global.ROOT_PATH+"manage.txt");
        BufferedReader br=new BufferedReader(fr);
        String databaseName="";
        String[] arrs=null;
        while ((databaseName=br.readLine())!=null) {
          if(databaseName.length()>0){
            Database tmpDB = new Database(databaseName);
            databases.put(databaseName,tmpDB);
            System.out.println("Manager has databases: "+databaseName);
          }
        }
        br.close();
        fr.close();
      }
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void createDatabaseIfNotExists(String dbName) {
    if (databases.containsKey(dbName)){
      System.out.println("Manager.java:createDatabaseIfNotExists: Database "+dbName+" exists.");
    }else{
      try{
        Database newDatabase = new Database(dbName);
        databases.put(dbName,newDatabase);
        File file = new File(Global.ROOT_PATH+"manage.txt");
        FileOutputStream fos = new FileOutputStream(file,true);
        String content = dbName+"\r\n";
        fos.write(content.getBytes());
        //create its corresponding directory
        File dir = new File(Global.ROOT_PATH+dbName);
        if (!dir.exists()) {
          dir.mkdir();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }

  public void deleteDatabase(String dbName) {
    if (!databases.containsKey(dbName)){
      System.out.println("Manager.java:cdeleteDatabase: Database "+dbName+" not exists.");
    }else{
      Database delDatabase = databases.get(dbName);
      delDatabase.drop();
      databases.remove(dbName);
      //delete the empty directory
      String dirPath = Global.ROOT_PATH+dbName;
      boolean success = (new File(dirPath)).delete();
      if (success) {
        System.out.println("Successfully deleted empty directory: " + dirPath);
      } else {
        System.out.println("Failed to delete empty directory: " + dirPath);
      }
    }
  }

  public Database switchDatabase(String dbName) {
    if(curDatabase!=null){
      Database lastDB = databases.get(curDatabase);
      lastDB.quit(); //存储到磁盘且回收内存池
      System.out.println("Last database: "+curDatabase);
    }
    if (!databases.containsKey(dbName)){
      System.out.println("Manager.java:switchDatabase: Database "+dbName+" not exists.");
    }else{
      Database curDB = databases.get(dbName);
      curDatabase = dbName;
      curDB.recover();
      System.out.println("Switch to database: "+ dbName);
      return curDB;
    }
    return null;
  }


  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}
