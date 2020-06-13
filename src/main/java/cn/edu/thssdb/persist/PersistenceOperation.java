package cn.edu.thssdb.persist;

import java.io.IOException;

public interface PersistenceOperation {
    public void storeTable(String tableName, byte[] data) throws IOException;

    public byte[] retrieveTable(String tableName);

    public void dropTable(String tableName) throws IOException;

}
