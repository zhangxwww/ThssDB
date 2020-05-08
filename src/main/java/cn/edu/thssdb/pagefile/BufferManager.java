package cn.edu.thssdb.pagefile;

import cn.edu.thssdb.pagefile.FrameDescription;
import cn.edu.thssdb.pagefile.Page;
import cn.edu.thssdb.utils.Global;

import java.awt.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


public class BufferManager implements PageFileConst {
    protected Page[] bufferPool;
    protected FrameDescription[] frameTab;
    protected HashMap<Integer, FrameDescription> pageMap;
    protected Replacer replacer;
    public HashMap<String, ArrayList<Integer>> tablePageMap;//记录了每张表有哪些页在buffer pool里
    private String databaseName;

    public BufferManager(String dbName, int numPages) {
        bufferPool = new Page[numPages];
        frameTab = new FrameDescription[numPages];
        for (int i = 0; i < numPages; i++) {
            bufferPool[i] = new Page();
            frameTab[i] = new FrameDescription(i);
        }
        pageMap = new HashMap<Integer, FrameDescription>(numPages);
        replacer = new Replacer(numPages);
        databaseName = dbName;
        tablePageMap = new HashMap<>(0);
    }




    public int newPage(Page firstPage, int runningSize) {
        return 0;
    }

    //ask for neededNum * (free pages)
    //this method doesn't include data write or read
    //it just tell the caller which pages can be used
    public ArrayList<Integer> allocatePages(int neededNum) {
//        System.out.println("Begin to allocate Pages.");
        ArrayList<Integer> pagesIdAllocated = new ArrayList<>();
        //First,find the free pages that haven't been allocated to a table
        int curNum = 0;
        int pageIndex = 0;
        while(curNum < neededNum && pageIndex < frameTab.length){
            if (frameTab[pageIndex].isEmpty()){
                pagesIdAllocated.add(pageIndex);
                curNum++;
                pageIndex++;
            }else{
                pageIndex++;
            }
        }
        //If not enough, then replacer is needed.
        if (curNum < neededNum){
            try{
                int[] replacedPages = replacer.pickVictims(frameTab,neededNum-curNum);
                for (int i: replacedPages){
                    pagesIdAllocated.add((Integer)i);
                    flushPage(i);//those replaced pages need to be written to disc
                }

            } catch (NoPageCanBeReplacedException | IllegalStateException e) {
                e.printStackTrace();
            }
        }
//        System.out.print("Finished. AllocatePages:");
//        printList(pagesIdAllocated);

        return pagesIdAllocated;
    }

    //When freePage is called, it means the original page is abandoned.
    //the content on it doesn't matter
    public void freePage(int pageIndex) {
        frameTab[pageIndex] = new FrameDescription(pageIndex);
    }

    //mark this page as pinned
    //pinned pages mean someone is using them.
    public void pinPage(int pageIndex) {
        if (pageIndex >= frameTab.length || pageIndex < 0){
            throw new IndexOutOfBoundsException("Try to pin Page #"+pageIndex+" In BufferManager.pinPage.");
        }
        frameTab[pageIndex].pinCount++;
    }

    //cover pages (with pagesId) with data
    //those pages should be marked as Dirty
    public void writePages(byte[] data, ArrayList<Integer> pagesId, String mTableName) {
        int pageNum = (int) Math.ceil((float)data.length / PageFileConst.PAGE_SIZE);
        if (pageNum != pagesId.size()){
            throw new IllegalArgumentException("data needs "+pageNum+" pages, but pagesId only contains "+pagesId.size()+" pages");
        }
        try{
            for (int i = 0; i < pageNum; i++){
                byte[] pageData = new byte[PAGE_SIZE];
                if (i < pageNum-1){
                    for (int j = 0; j < PAGE_SIZE ; j++){
                        pageData[j] = data[i*PAGE_SIZE+j];
                    }
                }else if (i == pageNum-1){
                    //最后一页的data可能是不满的
                    for (int j = 0; j < PAGE_SIZE &&  (i*PAGE_SIZE+j) < data.length; j++){
                        pageData[j] = data[i*PAGE_SIZE+j];
                    }

                }

                int pageIndex = pagesId.get(i);
                bufferPool[pageIndex].setContent(pageData);
                frameTab[pageIndex].isDirty = true;
                frameTab[pageIndex].frameNumber = i;
                //原本属于oldTable的页不再属于它了，所以修改tablePageMap
                String oldTable = frameTab[pageIndex].tableName;
                if (oldTable != null){
                    ArrayList<Integer> occupiedPages = tablePageMap.get(oldTable);
                    occupiedPages.remove((Integer) pageIndex);
                    tablePageMap.replace(oldTable,occupiedPages);
                }

                frameTab[pageIndex].tableName = mTableName;
                replacer.visitPage(pageIndex);
            }
        }catch (ArrayIndexOutOfBoundsException e){
            e.printStackTrace();
        }
    }

    public void pinPage(int pageNumber, Page page, boolean skipReading) {

    }

    public void unpinPage(int pageIndex) {
        if (pageIndex >= frameTab.length || pageIndex < 0){
            throw new IndexOutOfBoundsException("Try to unpin Page #"+pageIndex+" In BufferManager.unpinPage.");
        }
        if (frameTab[pageIndex].isPinned()){
            frameTab[pageIndex].pinCount--;
        }else{
            throw new IllegalStateException("Page not pinned");
        }
    }

    public void unpinPage(int pageNumber, boolean isDirty) {

    }

    public void flushAll() {
        for (int i = 0; i < bufferPool.length; i++){
            if(frameTab[i].isPinned()){
//                throw new IllegalStateException("In BufferManager.flushPage.");
            }else{
                DiskManager discM = new DiskManager(frameTab[i].tableName);

                if (frameTab[i].isDirty){
                    pinPage(i);
                    discM.writePage(frameTab[i].frameNumber,bufferPool[i].getContent());
                    frameTab[i].isDirty = false;
                    unpinPage(i);
                }
            }
        }
    }
    public void flushTable(String tableName){
        DiskManager discM = new DiskManager(Global.ROOT_PATH+databaseName+"/"+tableName);
        for (int i = 0; i < bufferPool.length; i++){
            if(frameTab[i].isPinned()){
//                throw new IllegalStateException("In BufferManager.flushPage.");
            }else{
                String name = frameTab[i].tableName;
                if ( name!=null &&name.equals(tableName)&&frameTab[i].isDirty){
                    pinPage(i);
                    discM.writePage(frameTab[i].frameNumber,bufferPool[i].getContent());
                    frameTab[i].isDirty = false;
                    unpinPage(i);
                }
            }
        }

    }

    protected void flushPage(int pageIndex){
        if(frameTab[pageIndex].isPinned()){
            throw new IllegalStateException("In BufferManager.flushPage.");
        }
        DiskManager discM = new DiskManager(frameTab[pageIndex].tableName);

        if (frameTab[pageIndex].isDirty){
            pinPage(pageIndex);
            discM.writePage(frameTab[pageIndex].frameNumber,bufferPool[pageIndex].getContent());
            frameTab[pageIndex].isDirty = false;
            unpinPage(pageIndex);
        }
    }

    public int getNumPages() {
        return bufferPool.length;
    }

    public int getNumUnpinned() {
        int count = 0;
        final int numPages = getNumPages();
        for (int i = 0; i < numPages; i++) {
            if (frameTab[i].pinCount == 0) {
                count++;
            }
        }
        return count;
    }

    public  void printList(List<Integer> i){
        for( Integer t: i){
            System.out.print(t+" ");
        }
        System.out.println();
    }

}
