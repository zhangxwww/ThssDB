package cn.edu.thssdb.pagefile;

public class Page implements PageFileConst {
    protected byte[] content;

    public Page() {
        content = new byte[PAGE_SIZE];
    }

    public Page(byte[] data) {
        setContent(data);
    }

    public void setContent(byte[] data) {
        if (data.length != PAGE_SIZE) {
            throw new IllegalArgumentException("Invalid page buffer length!");
        }
        this.content = data;
    }

    public byte[] getContent() {
        return content;
    }

    public void setPage(Page page) {
        this.content = page.content;
    }

    public void copyPage(Page page) {
        System.arraycopy(page.content, 0, this.content, 0, PAGE_SIZE);
    }

    public char getCharVal(int offset) {
        return (char) (this.content[offset]);
    }

    public void setCharVal(int offset, char value) {
        this.content[offset] = (byte) value;
    }

    public int getIntVal(int offset) {
        int result = 0;
        for (int i = 0; i < 4; i++) {
            int byteint = this.content[offset + i] & 0xff;
            result = result | (byteint << ((3 - i) * 8));
        }
        return result;
    }

    public void setIntVal(int offset, int value) {
        for (int i = 0; i < 4; i++) {
            this.content[offset + i] = (byte) ((value >> ((3 - i) * 8) & 0xff));
        }
    }

    public long getLongVal(int offset) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            long byteint = this.content[offset + i] & 0xff;
            result = result | (byteint << ((7 - i) * 8));
        }
        return result;
    }

    public void setLongVal(int offset, long value) {
        for (int i = 0; i < 8; i++) {
            this.content[offset + i] = (byte) ((value >> ((7 - i) * 8) & 0xff));
        }
    }

    public float getFloatVal(int offset) {
        return Float.intBitsToFloat(getIntVal(offset));
    }

    public void setFloatVal(int offset, float value) {
        setIntVal(offset, Float.floatToIntBits(value));
    }

    public double getDoubleVal(int offset) {
        return Double.longBitsToDouble(getLongVal(offset));
    }

    public void setDoubleVal(int offset, double value) {
        setLongVal(offset, Double.doubleToLongBits(value));
    }

    public String getStringVal(int offset, int length) {
        int actualLength = length;
        int bufferLength = this.content.length - offset;
        if (bufferLength < length) {
            actualLength = bufferLength;
        }
        return new String(this.content, offset, actualLength).trim();
    }

    public void setStringVal(int offset, String value) {
        byte[] valueBytes = value.getBytes();
        System.arraycopy(valueBytes, 0, this.content, offset, valueBytes.length);
    }
}