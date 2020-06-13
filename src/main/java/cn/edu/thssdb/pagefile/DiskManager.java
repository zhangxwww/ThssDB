package cn.edu.thssdb.pagefile;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class DiskManager implements PageFileConst {
    private String bufferPath;
    private int maxFrameNumber;
    public DiskManager(String bufferPath, int maxPageFrameNumber) {
        this.bufferPath = bufferPath;
        this.maxFrameNumber = maxPageFrameNumber;
    }

    public int allocatePage() {
        byte[] nullByte = new byte[PAGE_SIZE];
        Arrays.fill(nullByte, (byte)0);
        maxFrameNumber++;
        writePage(maxFrameNumber, nullByte);
        return maxFrameNumber;
    }

    public void deallocatePage(int frameNumber) throws IOException {
        // do nothing
    }

    void writePage(int frameNumber, byte[] data) {
        int fileNumber = (int)(frameNumber / NUM_FILE_PAGES);
        int bias = frameNumber % NUM_FILE_PAGES;
        int offset = bias * PAGE_SIZE;
        String fileName = this.bufferPath + fileNumber;
        File dest = new File(fileName);
        try {
            try (FileOutputStream outputStream = new FileOutputStream(dest, false)) {
                FileChannel channel = outputStream.getChannel();
                channel.position(offset);
                channel.write(ByteBuffer.wrap(data));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] readPage(int frameNumber) {
        int fileNumber = (int)(frameNumber / NUM_FILE_PAGES);
        int bias = frameNumber % NUM_FILE_PAGES;
        if (bias == 0) {
            fileNumber -= 1;
            bias = NUM_FILE_PAGES;
        }
        int offset = (bias - 1) * PAGE_SIZE;
        String fileName = this.bufferPath + fileNumber;
        File src = new File(fileName);
        byte[] result = null;
        try {
            InputStream fileInputStream = new FileInputStream(src);
            fileInputStream.skip(offset);
            BufferedInputStream inputStream = new BufferedInputStream(fileInputStream);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] flush = new byte[PAGE_SIZE];
            int len = -1;
            while ((len = inputStream.read(flush)) != -1) {
                outputStream.write(flush, 0, len);
            }
            outputStream.flush();
            result = outputStream.toByteArray();
            inputStream.close();
            outputStream.close();
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
