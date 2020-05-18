package cn.edu.thssdb.pagefile;




import cn.edu.thssdb.exception.NoPageCanBeReplacedException;

import java.util.LinkedList;

//This replacer uses LRU algorithm.
public class Replacer {
    private LinkedList<Integer> lruInfo;
    int maxSize;
    public Replacer(int size){
        lruInfo = new LinkedList<>();
        maxSize = size;
    }

    // @return: -1 means error
    //          0 means OK
    public void visitPage(int pageNumber) throws IndexOutOfBoundsException{
        if (pageNumber >= maxSize || pageNumber < 0) {
            throw new IndexOutOfBoundsException("Wrong page number "+ pageNumber + " in Replcaer.visitPage");
        }
        lruInfo.remove((Integer)pageNumber);
        lruInfo.addFirst((Integer)pageNumber);

    }

    public void printList(){
        for(Integer i:lruInfo){
            System.out.print(i+" ");
        }
        System.out.println();
    }

    public int[] pickVictims(FrameDescription[] frameTab, int victimNum) throws NoPageCanBeReplacedException {
        int[] victimId = new int[victimNum];
        int curVictimNum = 0;
        int index = lruInfo.size()-1;
        while(curVictimNum < victimNum && index >= 0){
            int pageId = lruInfo.get(index);
            if (frameTab[pageId].isPinned()){
                // pinned page must stay in buffer pool
                index--;
            }else{
                index--;
                victimId[curVictimNum++] = pageId;
            }
        }
        if (curVictimNum < victimNum){
            throw new NoPageCanBeReplacedException();
        }

        for (int i: victimId){
            System.out.print(i+" ");
        }
        System.out.println();
        
        return victimId;
    }



    public static void main(String[] args) {
        int[] set = {1,2,3,4,5,6,7,8,9,10};
        int[] set2 = {1,2,3,4,0,3,0,1,2,1};

        Replacer test = new Replacer(5);
        for (int i:set2){
            test.visitPage(i);

        }
        test.printList();
        FrameDescription[] fd = new FrameDescription[5];
        for (int i = 0; i < 5; i++) {
            fd[i] = new FrameDescription(i);
        }

        try{
            test.pickVictims(fd,3);
        }catch (NoPageCanBeReplacedException e) {
            e.printStackTrace();
        }

    }

}
