package cn.edu.thssdb.pagefile;

import cn.edu.thssdb.exception.NoPageCanBeReplacedException;

import java.util.LinkedList;

//This replacer uses LRU algorithm.
public class Replacer {
    private LinkedList<Integer> lruInfo;
    int maxSize;

    public Replacer(int size) {
        lruInfo = new LinkedList<>();
        maxSize = size;
    }

    // @return: -1 means error
    //          0 means OK
    public void visitPage(int pageNumber) throws IndexOutOfBoundsException {
        if (pageNumber >= maxSize || pageNumber < 0) {
            throw new IndexOutOfBoundsException("Wrong page number " + pageNumber + " in Replcaer.visitPage");
        }
        lruInfo.remove((Integer) pageNumber);
        lruInfo.addFirst((Integer) pageNumber);

    }

    public void printList() {
        for (Integer i : lruInfo) {
            System.out.print(i + " ");
        }
        System.out.println();
    }

    public int pickVictim(FrameDescription[] frameTab) throws NoPageCanBeReplacedException {
        int index = lruInfo.size() - 1;
        while (index >= 0) {
            int pageId = lruInfo.get(index);
            if (frameTab[pageId].isPinned()) {
                // pinned page can't be replaced
                index--;
            }else{
               return pageId;
            }
        }
        throw new NoPageCanBeReplacedException();
    }
}
