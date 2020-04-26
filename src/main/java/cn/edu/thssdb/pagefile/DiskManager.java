package cn.edu.thssdb.pagefile;

import java.io.*;
import java.util.Arrays;

public class DiskManager implements PageFileConst {
    private String bufferPath;

    public DiskManager(String bufferPath) {
        this.bufferPath = bufferPath;
    }

    public void allocatePage(int frameNumber) {
        byte[] nullByte = new byte[PAGE_SIZE];
        Arrays.fill(nullByte, (byte)0);
        writePage(frameNumber, nullByte);
    }

    public void deallocatePage(int frameNumber) throws IOException {
        String fileName = this.bufferPath + String.valueOf(frameNumber);
        File dest = new File(fileName);
        if (!dest.exists() || dest.isFile()) {
            throw new IllegalArgumentException("Invalid page to delete from disk!");
        }
        else {
            if (!dest.delete()) {
                throw new IOException("Fail to delete page from disk!");
            }
        }
    }

    public void writePage(int frameNumber, byte[] data) {
        String fileName = this.bufferPath + String.valueOf(frameNumber);
        File dest = new File(fileName);
        try {
            InputStream inputStream = new ByteArrayInputStream(data);
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(dest, false));
            byte[] flush = new byte[PAGE_SIZE];
            int len = -1;
            while ((len = inputStream.read(flush)) != -1) {
                outputStream.write(flush, 0, len);
            }
            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] readPage(int frameNumber) {
        String fileName = this.bufferPath + String.valueOf(frameNumber);
        File src = new File(fileName);
        byte[] result = null;
        try {
            InputStream inputStream = new BufferedInputStream(new FileInputStream(src));
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] flush = new byte[PAGE_SIZE];
            int len = -1;
            while ((len = inputStream.read(flush)) != -1) {
                outputStream.write(flush, 0, len);
            }
            outputStream.flush();
            result = outputStream.toByteArray();
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
