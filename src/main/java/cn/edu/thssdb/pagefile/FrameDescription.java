package cn.edu.thssdb.pagefile;

public class FrameDescription implements PageFileConst{
    public int index;
    public int frameNumber;
    public int pinCount;
    public boolean isDirty;
    public int state;


    public FrameDescription(int index) {
        this.index = index;
        this.frameNumber = INVALID_PAGEID;
        this.pinCount = 0;
        this.isDirty = false;
        this.state = 0;
    }

    public boolean isPinned(){return pinCount != 0;}

    public boolean isEmpty(){return frameNumber == INVALID_PAGEID;}

}
