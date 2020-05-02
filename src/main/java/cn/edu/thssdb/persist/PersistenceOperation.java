package cn.edu.thssdb.persist;

public interface PersistenceOperation {
    public void storeTable(String tableName, byte[] data);

    public byte[] retrieveTable(String tableName);

}
