package cn.edu.thssdb.persist;

import cn.edu.thssdb.pagefile.PageFileConst;
import cn.edu.thssdb.pagefile.BufferManager;
import cn.edu.thssdb.pagefile.DiskManager;

import cn.edu.thssdb.pagefile.FrameDescription;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.utils.Global;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class PageFilePersist implements PersistenceOperation {
	public HashMap<String, ArrayList<Integer>> tableFramePagesId;
	private BufferManager bfm;
	private DiskManager dsm;
	private HashMap<String, TableInfo> tableInfos;

	// initialize buffer pool manager and disk manager
	public PageFilePersist(String dbName, int numPages, HashMap<String, TableInfo> tableInfos, int maxPageFrameNumber) {
		this.dsm = new DiskManager(Global.ROOT_PATH + dbName, maxPageFrameNumber);
		this.bfm = new BufferManager(dbName, numPages, dsm);
		this.tableInfos = tableInfos;
	}

	// write table to buffer pool and persist the buffer pool
	@Override
	public void storeTable(String tableName, byte[] data) throws IOException {
		// a reference to the table infos stored in database class
		ArrayList<Integer> pages = this.tableInfos.get(tableName).framePagesId;
		int numPages = pages.size();
		int pageNumNeeded = (int) Math.ceil((float) (data.length) / PageFileConst.PAGE_SIZE);

		// if the table shrinks and don't need so many pages
		if (pageNumNeeded < numPages) {
			int numPagesToDelete = numPages - pageNumNeeded;
			ArrayList<Integer> pagesDeleted = new ArrayList<>();
			for (int i = 0; i < numPagesToDelete; i++) {
				int pageToDelete = pages.remove(tableInfos.size() - 1);
				dsm.deallocatePage(pageToDelete);
				pagesDeleted.add(pageToDelete);
			}
			bfm.clearDeallocatedDiskImages(pagesDeleted);
		}
		// otherwise
		for (int i = 0; i < pageNumNeeded; i++) {
			byte[] pageData = new byte[PageFileConst.PAGE_SIZE];
			System.arraycopy(data, i * PageFileConst.PAGE_SIZE, pageData, 0, PageFileConst.PAGE_SIZE);
			if (i < numPages) {
				// override existed disk page frames
				bfm.writeExistedPage(pageData, pages.get(i));
			} else {
				// use buffer manager to allocate new page frames and write the data
				int newFrameNumber = dsm.allocatePage();
				pages.add(newFrameNumber);
				bfm.writeNewPage(pageData, newFrameNumber);
			}
		}
	}

	// flush the buffer pool to disk page frames
	public void flush() {
		bfm.flush();
	}

	// use disk manager to retrieve pages to array buffers and return the buffer
	@Override
	public byte[] retrieveTable(String tableName) {
		ArrayList<Integer> tablePages = tableInfos.get(tableName).framePagesId;
		int frameNum = tablePages.size();
		byte[] data = new byte[PageFileConst.PAGE_SIZE * frameNum];
		for (int i = 0; i < frameNum; i++) {
			int p = tablePages.get(i);
			byte[] buf = dsm.readPage(p);
			int start = i * PageFileConst.PAGE_SIZE;
			System.arraycopy(buf, 0, data, start, PageFileConst.PAGE_SIZE);
		}
		return data;
	}

	public void dropTable(String tableName) throws IOException {
		ArrayList<Integer> pages = tableInfos.get(tableName).framePagesId;
		for (int p : pages) {
			dsm.deallocatePage(p);
		}
		bfm.clearDeallocatedDiskImages(pages);
	}
}
