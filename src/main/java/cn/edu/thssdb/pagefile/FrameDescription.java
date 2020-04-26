package cn.edu.thssdb.pagefile;

public class FrameDescription implements PageFileConst{
    public int index;
    public int pageNumber;
    public int pinCount;
    public boolean isDirty;
    public int state;

    public FrameDescription(int index) {
        this.index = index;
        this.pageNumber = INVALID_PAGEID;
        this.pinCount = 0;
        this.isDirty = false;
        this.state = 0;
    }
}
