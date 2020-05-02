#### 存储模块

> persist

提供持久化和存储管理的接口。

主要提供了这样两个接口：

```JAVA
//每个database有一个自己的persistManager
//其中主要记录了每张表在内存中占据了哪些页
persistManager.storeTable(tableName,bData);
//bData是记录的序列化得到的字节流
```

```JAVA
persistManager.retrieveTable(tableName);
//得到表的记录经序列化后的字节流
```

> pagefile  

实现页式存储的管理。采用的是LRU置换算法。

#### Schema

> table

```JAVA
Table.serialize(PageFilePersist persistManager) 
//将记录存到bufferPool中
Table.persist(PageFilePersist persistManager) 
//持久化，存储到磁盘文件
Table.deserialize(PageFilePersist persistManager) 
//从磁盘文件得到记录ArrayList<Row>
```

