package cn.edu.thssdb.pagefile;

import cn.edu.thssdb.pagefile.FrameDescription;
import cn.edu.thssdb.pagefile.Page;

import java.awt.*;
import java.util.HashMap;

public class BufferManager implements PageFileConst {
    protected Page[] bufferPool;
    protected FrameDescription[] frameTab;
    protected HashMap<Integer, FrameDescription> pageMap;

    public BufferManager(int numPages) {
        bufferPool = new Page[numPages];
        frameTab = new FrameDescription[numPages];
        for (int i = 0; i < numPages; i++) {
            bufferPool[i] = new Page();
            frameTab[i] = new FrameDescription(i);
        }
        pageMap = new HashMap<Integer, FrameDescription>(numPages);
    }

    public int newPage(Page firstPage, int runningSize) {
        return 0;
    }

    public void freePage(int pageNumber) {

    }

    public void pinPage(int pageNumber, Page page, boolean skipReading) {

    }

    public void unpinPage(int pageNumber, boolean isDirty) {

    }

    public void flushAll() {

    }

    protected void flushPage(int pageNumber) {

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
}
