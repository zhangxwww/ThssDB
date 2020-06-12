package cn.edu.thssdb.service;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;

public class LogHandler {
    private static String WALpath = null;
    private Database database = null;

    public LogHandler(Database database) {
        WALpath = Global.ROOT_PATH + database.getName() + "/" + database.getName()  + ".log";
        this.database = database;
    }

    public void addCreateTableLog(long transactionID, String tableName) {
        String line = String.valueOf(transactionID) + " " + "CREATE" + " " + tableName;
        writeWAL(line);
    }

    public void addDropTableLog(long transactionID, String tableName) {
        String line = String.valueOf(transactionID) + " " + "DROP" + " " + tableName;
        writeWAL(line);
    }

    public void addInsertRowLog(long transactionID, String tableName, String primaryKey, String primaryValue) {
        //只需要记录新插入行的主键即可
        String line = String.valueOf(transactionID) + " " + "INSERT" + " " + tableName +
                " " + primaryKey + " " + primaryValue;
        writeWAL(line);

    }

    public void addDeleteRowLog(long transactionID, String tableName, int attrNum, String[] attrValues) {
        String line = String.valueOf(transactionID) + " " + "DELETE" + " " + tableName +
                " " + attrNum + " ";
        for (String s : attrValues) {
            line += s + " ";
        }
        writeWAL(line);
    }

    void writeWAL(String line) {
        line += "\r\n";
        try {
            File file = new File(WALpath);
            if (!file.exists()) {
                boolean success = file.createNewFile();
                if (!success) {
                    throw new IOException("Creating WAL failed.");
                }
            }
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            fileOutputStream.write(line.getBytes());
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void commit(long transactionID) {
        //把database persist，但是如果有drop table 语句，还要drop table
        ArrayList<String> remainlines = new ArrayList<>(0);
        try {
            File file = new File(WALpath);
            if (!file.exists()) {
                throw new IOException("WAL file doesn't exist.");
            } else {
                FileReader fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr);
                String line = null;
                while ((line = br.readLine()) != null) {
                    if (line.length() > 0) {
                        String[] strMsg = line.split(" ");
                        long tmpTranID = Long.parseLong(strMsg[0]);
                        String cmd = strMsg[1];
                        if (tmpTranID == transactionID) {
                            if (cmd.equals("DROP")) {
                                String tbName = strMsg[2];
                                database.dropTable(tbName);
                                System.out.println("DROP " + tbName);
                            }
                        } else {
                            //非本事务的log要保留
                            remainlines.add(line);
                        }
                    }
                }
                database.persist(); //to disc
                br.close();
                fr.close();
                //别的事务的语句要写回
                FileOutputStream fileWriter = new FileOutputStream(file, false);

                String remain = "";
                for (String remainLine : remainlines) {
                    if (remainLine.length() > 0) {
                        remain += remainLine + "\r\n";
                    }

                }
                fileWriter.write(remain.getBytes());
                fileWriter.flush();
                fileWriter.close();


            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getPath() {
        return WALpath;
    }

}
