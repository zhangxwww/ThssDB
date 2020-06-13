package cn.edu.thssdb.pagefile;

public class FrameDescription implements PageFileConst{
    public int index;
    int frameNumber;
    int pinCount;
    boolean isDirty;
    private int state;


    FrameDescription(int index) {
        this.index = index;
        this.frameNumber = INVALID_PAGEID;
        this.pinCount = 0;
        this.isDirty = false;
        this.state = 0;
    }

    boolean isPinned(){return pinCount != 0;}

    boolean isEmpty(){return frameNumber == INVALID_PAGEID;}

    void setFrameNumber(int frameNumber) {
        this.frameNumber = frameNumber;
    }

    void setDirty(boolean dirty) {
        isDirty = dirty;
    }

    int getFrameNumber() {
        return frameNumber;
    }

    public boolean isDirty() {
        return isDirty;
    }
}
