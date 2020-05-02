package cn.edu.thssdb.persist;

import java.io.*;

public class NaiveSerializationPersist implements PersistenceOperation {


    /*
    *
    */
    @Override
    public void storeTable(String tableName, byte[] data) {
        try{
//            File file = new File(filePath);
            FileOutputStream fOutStream = new FileOutputStream(tableName, false);
            fOutStream.write(data);
            fOutStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] retrieveTable(String tableName) {
        byte[] data = new byte[0];
        try{
            FileInputStream fInStream = new FileInputStream(tableName);
            int size = fInStream.available();// byte size
            data=new byte[size];
            int length = fInStream.read(data);
            System.out.println("retrieve" + tableName + ": " + length + "bytes");
            fInStream.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }
}
