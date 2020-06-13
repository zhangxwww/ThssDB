package cn.edu.thssdb.pagefile;

import cn.edu.thssdb.exception.NoPageCanBeReplacedException;
import cn.edu.thssdb.schema.TableInfo;
import cn.edu.thssdb.utils.Global;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class BufferManager implements PageFileConst {
    private Page[] bufferPool;
    private FrameDescription[] frameTab;
    private int size;
    private Replacer replacer;
    private DiskManager diskManager;
    private ArrayList<Integer> availablePages = new ArrayList<>();

    public BufferManager(String dbName, int numPages, DiskManager dsm) {
        bufferPool = new Page[numPages];
        frameTab = new FrameDescription[numPages];
        for (int i = 0; i < numPages; i++) {
            bufferPool[i] = new Page();
            frameTab[i] = new FrameDescription(i);
            availablePages.add(i);
        }
        replacer = new Replacer(numPages);
        size = numPages;
        diskManager = dsm;
    }

    private int allocatePage() {
        // First,find the free pages that haven't been allocated to a table
        if (availablePages.size() > 0) {
            return availablePages.remove(availablePages.size() - 1);
        } else {
            try {
                int replacedPage = replacer.pickVictim(frameTab);
                pinPage(replacedPage);
                flushPage(replacedPage);
                unpinPage(replacedPage);
                return replacedPage;
            } catch (NoPageCanBeReplacedException | IllegalStateException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    // mark this page as pinned so that it cannot be altered
    private void pinPage(int pageIndex) {
        if (pageIndex >= frameTab.length || pageIndex < 0){
            throw new IndexOutOfBoundsException("Try to pin Page #" + pageIndex + " In BufferManager.pinPage.");
        }
        frameTab[pageIndex].pinCount++;
    }

    public void writeExistedPage(byte[] data, int frameNumber) {
        boolean cacheHit = false;
        for (int i = 0; i < size; i++) {
            if (frameTab[i].frameNumber == frameNumber) {
                // cache hit
                cacheHit = true;
                if (!bufferPool[i].compareContent(data)) {
                    replacer.visitPage(i);
                    pinPage(i);
                    bufferPool[i].setContent(data);
                    frameTab[i].setDirty(true);
                    unpinPage(i);
                }
                break;
            }
        }
        if (!cacheHit) {
            int pageId = allocatePage();
            pinPage(pageId);
            frameTab[pageId].setFrameNumber(frameNumber);
            frameTab[pageId].setDirty(true);
            bufferPool[pageId].setContent(data);
            replacer.visitPage(pageId);
            unpinPage(pageId);
        }
    }

    public void writeNewPage(byte[] data, int frameNumber) {
        int pageId = allocatePage();
        if (pageId != -1) {
            pinPage(pageId);
            frameTab[pageId].setFrameNumber(frameNumber);
            frameTab[pageId].setDirty(true);
            bufferPool[pageId].setContent(data);
            replacer.visitPage(pageId);
            unpinPage(pageId);
        } else {
            System.out.println("Unable to allocate new pages");
        }
    }

    public void clearDeallocatedDiskImages(ArrayList<Integer> pages) {
        for (int i = 0; i < size; i++) {
            if (pages.contains(frameTab[i].frameNumber)) {
                pinPage(i);
                frameTab[i].setDirty(false);
                frameTab[i].setFrameNumber(PageFileConst.INVALID_PAGEID);
                unpinPage(i);
            }
        }
    }

    private void unpinPage(int pageIndex) {
        if (pageIndex >= frameTab.length || pageIndex < 0){
            throw new IndexOutOfBoundsException("Try to unpin Page #"+pageIndex+" In BufferManager.unpinPage.");
        }
        if (frameTab[pageIndex].isPinned()){
            frameTab[pageIndex].pinCount--;
        }else{
            throw new IllegalStateException("Page not pinned");
        }
    }

    public void flush(){
        for (int i = 0; i < size; i++) {
            flushPage(i);
        }
    }

    private void flushPage(int pageNumber) {
        FrameDescription frameDescription = frameTab[pageNumber];
        int frameNumber = frameTab[pageNumber].getFrameNumber();
        pinPage(pageNumber);
        if (frameDescription.isDirty) {
            diskManager.writePage(frameNumber, bufferPool[pageNumber].getContent());
        }
        unpinPage(pageNumber);
    }
}
