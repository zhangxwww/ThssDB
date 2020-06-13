package cn.edu.thssdb.schema;


import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.TableAlreadyExistsException;
import cn.edu.thssdb.exception.TableNotExistsException;
import cn.edu.thssdb.persist.PageFilePersist;
import cn.edu.thssdb.query.WhereCondition;
import cn.edu.thssdb.service.LogHandler;
import cn.edu.thssdb.service.StatementAdapter;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

	private String name;
	private HashMap<String, Table> tables;
	ReentrantReadWriteLock lock;
	private HashMap<String, TableInfo> tableInfos;
	public PageFilePersist persistManager;

	boolean isRecovered = false;
    int maxPageFrameNumber = -1;

	public Database(String name) {
		this.name = name;
		this.tables = new HashMap<>();
		this.lock = new ReentrantReadWriteLock();

	}

	public void persist() {
		// store table
		for (Table t : tables.values()) {
			byte[] bData = t.serialize();
			try {
				persistManager.storeTable(t.tableName, bData); //to buffer pool
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
        persistManager.flush(); // flush the buffer pool to disk

		// store table infos (meta data)
		try {
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(Global.ROOT_PATH + name + "/manage", false));
			objectOutputStream.writeInt(maxPageFrameNumber);
			for (TableInfo tbInfo : tableInfos.values()) {
				objectOutputStream.writeObject(tbInfo);
			}
			objectOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Table createTable(String tbName, Column[] columns) {
		if (tables.containsKey(tbName)) {
			throw new TableAlreadyExistsException();
		} else {
			Table newTable = new Table(name, tbName, columns);
			tables.put(tbName, newTable);
			TableInfo newTableInfo = new TableInfo(tbName, newTable.getColumns(), newTable.getPrimaryKeyIndex(), new ArrayList<>());
			tableInfos.put(tbName, newTableInfo);
			return newTable;
		}
	}

	public void dropTable(String tbName) {
		// TODO
		try {
			if (!tables.containsKey(tbName)) {
				System.out.println("Table " + tbName + " not exists");
				throw new TableNotExistsException();
			} else {
				persistManager.dropTable(tbName);
				tables.remove(tbName);
				tableInfos.remove(tbName);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// drop database
	public void drop() {
		try {
			File dir = new File(Global.ROOT_PATH + name);
			if (dir.isDirectory()) {
				String[] children = dir.list();
				for (int i = 0; i < children.length; i++) {
					File delFile = new File(dir, children[i]);
					boolean success = delFile.delete();
					if (!success)
						throw new IOException("Deleting" + children[i] + "failed.");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public void recover() throws IOException {
		if (isRecovered) return;

		// recover meta data from disk file `manage`
		ObjectInputStream ois = null;
		tableInfos = new HashMap<>(0);
		tables.clear();
		try {
			File manageFile = new File(Global.ROOT_PATH + name + "/manage");
			if (!manageFile.exists()) {
				manageFile.createNewFile();
			} else {
				ois = new ObjectInputStream(new FileInputStream(Global.ROOT_PATH + name + "/manage"));
				maxPageFrameNumber = ois.readInt();
				while (true) {
					TableInfo tbInfo = (TableInfo) ois.readObject();
					Column[] cols = new Column[tbInfo.columns.size()];
					for (int i = 0; i < cols.length; i++) {
						cols[i] = tbInfo.columns.get(i);
					}
					Table newTable = new Table(name, tbInfo.tableName, cols, tbInfo.primaryIndex);
					tables.put(tbInfo.tableName, newTable);
					tableInfos.put(tbInfo.tableName, tbInfo);
				}

			}

		} catch (EOFException e) {
			System.out.println("读取数据库的表信息： 类对象已完全读入");
		} catch (FileNotFoundException e) {

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			isRecovered = true;
			if (ois != null) {
				ois.close();
			}
			persistManager = new PageFilePersist(name, Global.BUFFER_POOL_PAGE_NUM, tableInfos);
			System.out.println("recover database: " + name);

			System.out.println("tableInfo: ");
			for (TableInfo tb : tableInfos.values()) {
				System.out.println(tb.tableName + tb.columns.toString());
			}
			for (Table t : tables.values()) {
				byte[] bData = persistManager.retrieveTable(t.tableName);
				t.recover(bData); //表获取数据
			}
			this.recoverUncommittedCmd(0);
		}
	}

	public String getName() {
		return name;
	}

	public void quit() {
		persist();
		persistManager = null;
	}

	public Table getTable(String tbName) {
		Table t = tables.get(tbName);
		if (t == null) {
			throw new TableNotExistsException();
		}
		return t;
	}

	public void recoverUncommittedCmd(long transactionId) {
		//需要倒序处理
		LogHandler logHandler = new LogHandler(this);
		ArrayList<String> uncommittedCmds = new ArrayList<>(0);
		StatementAdapter adapter = new StatementAdapter(this, 0);
		StringBuilder remain = new StringBuilder();
		try {
			File file = new File(logHandler.getPath());
			if (file.exists()) {
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				String line = null;
				while ((line = br.readLine()) != null) {
					if (line.length() > 0) {
						uncommittedCmds.add(line);
					}
				}
				//开始恢复
				int cmdNum = uncommittedCmds.size();
				for (int i = cmdNum - 1; i >= 0; i--) {
					String[] strMsg = uncommittedCmds.get(i).split(" ");
					Long logTransactionId = Long.parseLong(strMsg[0]);
					if (logTransactionId != transactionId && transactionId != 0) {
						remain.append(uncommittedCmds.get(i)).append("\r\n");
						continue;
					}
					String cmd = strMsg[1];
					String tbName = strMsg[2];
					if (cmd.equals("CREATE")) {
						this.dropTable(tbName);
					} else if (cmd.equals("DROP")) {
						//由于目前在adpater里，对于事务状态下的drop就是什么也没动，所以这个就不用处理了
						continue;
					} else if (cmd.equals("DELETE")) {
						int attrNum = Integer.parseInt(strMsg[3]);
						String[] attrValues = new String[attrNum];
						for (int j = 0; j < attrNum; j++) {
							attrValues[j] = strMsg[4 + j];
						}
						try {
							adapter.insertTableRow(tbName, attrValues);
						} catch (DuplicateKeyException ignored) {
						}
					} else if (cmd.equals("INSERT")) {
						String primaryKey = strMsg[3];
						String primaryVal = strMsg[4];
						WhereCondition wc = new WhereCondition("=", tbName, primaryKey, primaryVal);
						try {
							adapter.delFromTable(tbName, wc);
						} catch (KeyNotExistException ignored) {
						}
					} else {
						System.out.println("Wrong Cmd");
					}
				}
				this.persist(); //to disc
				br.close();
				fr.close();

				//清空该文件
				FileOutputStream fileWriter = new FileOutputStream(file, false);
				fileWriter.write(remain.toString().getBytes());
				fileWriter.flush();
				fileWriter.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
