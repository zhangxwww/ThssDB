package cn.edu.thssdb.persist;
import cn.edu.thssdb.pagefile.PageFileConst;
import cn.edu.thssdb.pagefile.BufferManager;
import cn.edu.thssdb.pagefile.DiskManager;

import cn.edu.thssdb.pagefile.FrameDescription;

import java.util.ArrayList;
import java.util.HashMap;


public class PageFilePersist implements PersistenceOperation{
    /* When a user joins to a database, infos of those tables in this database will be put in the HashMap.

    tablePageMap include: tableName and which pages in the buffer pool have been occupied by it.
    tableFramesNum include: tableName and how many frames does it have.
    At the beginning, all tables don't occupy pages in the buffer pool.
     */
    //之后有了database的基础，就可以改为private，用方法来赋值下面两个hash映射表
    public HashMap<String, Integer> tableFramesNum;
    public HashMap<String, ArrayList<Integer>> tablePageMap;
    private BufferManager bfm;

    //理应还需要databse的一些信息：有哪些表，每个表有多少frame
    public PageFilePersist(int numPages){
        bfm = new BufferManager(numPages);
        tableFramesNum = new HashMap<>();
        tablePageMap = new HashMap<>();

    }


    public void setTablePageMap() {
        bfm.setTablePageMap(tablePageMap);
    }

    //table record -> memory
    @Override
    public void storeTable(String tableName, byte[] data) {

        if (!tablePageMap.containsKey(tableName)){
            throw new IllegalArgumentException("Try to store table "+tableName+" which is unexisted.");
        }
        ArrayList<Integer> pagesInBufferPool = tablePageMap.get(tableName);
        ArrayList<Integer> pagesAllocated = new ArrayList<>();

        //调试信息
        System.out.print(tableName+"原本占据的页为： ");
        for (Integer integer : pagesInBufferPool) {
            System.out.print(integer + " ");
        }
        System.out.println();


        int pageNumNeeded = (int) Math.ceil((float)data.length / PageFileConst.PAGE_SIZE);
        int pageNumHad = pagesInBufferPool.size();
        if (pageNumHad >= pageNumNeeded){
            int index = 0;
            for (; index < pageNumNeeded; index++){
                pagesAllocated.add(pagesInBufferPool.get(index));
            }
            for (; index < pageNumHad; index++){
                bfm.freePage(pagesInBufferPool.get(index));
                System.out.print("Free page "+pagesInBufferPool.get(index));
            }
            for (Integer i:pagesAllocated){
                bfm.pinPage(i);
                // Mark those neededPages as not free, to write into those pages
            }
        }else{
            int index = 0;
            for (Integer i:pagesInBufferPool){
                bfm.pinPage(i);
                // Mark those neededPages as not free, to avoid during allocating
                // duplicate pages are allocated.
            }
            ArrayList<Integer> pagesId = bfm.allocatePages(pageNumNeeded-pageNumHad);
            for (Integer i:pagesId){
                bfm.pinPage(i);
                // Mark those neededPages as not free, to write into those pages
            }
            pagesAllocated.addAll(pagesInBufferPool);
            pagesAllocated.addAll(pagesId);
        }

        bfm.writePages(data,pagesAllocated,tableName);

        for (Integer i : pagesAllocated) {
            bfm.unpinPage(i);
        }

        tablePageMap.replace(tableName,pagesAllocated);
        tableFramesNum.replace(tableName,pagesAllocated.size());
        //调试信息
        System.out.print(tableName+"新的占据的页为： ");
        for (Integer integer : pagesAllocated) {
            System.out.print(integer + " ");
        }
        System.out.println();
    }

    //table record memory-> disc
    public void flushTable(String tableName){
        bfm.flushTable(tableName);
    }

    //from disc -> memory and get all data
    @Override
    public byte[] retrieveTable(String tableName) {

        if (!tableFramesNum.containsKey(tableName)){
            throw new IllegalArgumentException("Try to retrieve table "+tableName+" which is unexisted.");
        }

        int frameNum = tableFramesNum.get(tableName);

        byte[] data = new byte[PageFileConst.PAGE_SIZE * frameNum];
        DiskManager dsm = new DiskManager(tableName);
        for (int i = 0; i < frameNum; i++){
            byte[] dataTmp = dsm.readPage(i);
            for (int j = 0; j < PageFileConst.PAGE_SIZE; j++){
                int start = i*PageFileConst.PAGE_SIZE;
                data[start+j] = dataTmp[j];
            }
        }

        System.out.println("Retrieve: "+ tableName+" needs "+frameNum+" pages.");
        ArrayList<Integer> pagesId = bfm.allocatePages(frameNum);
        for(Integer i: pagesId){
            bfm.pinPage(i);
        }
        //write data to buffer pool
        bfm.writePages(data,pagesId,tableName);

        for(Integer i: pagesId){
            bfm.unpinPage(i);
        }
        tablePageMap.put(tableName,pagesId);
        return data;
    }
}
