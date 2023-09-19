package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataChannel {

    private FileChannel outputNio;

    private OutputStream outputBio;

    private ByteBuffer lastBuffer;

    private final boolean useNio;

    public DataChannel(File dataFile, boolean useNio, int nioBuffersSize, int bioBufferSize) throws FileNotFoundException {
        this.useNio = useNio;
        if (useNio) {
            outputNio = new FileOutputStream(dataFile, true).getChannel();
            lastBuffer = ByteBuffer.allocateDirect(nioBuffersSize);
        } else {
            outputBio = new BufferedOutputStream(new FileOutputStream(dataFile, true), bioBufferSize);
        }
    }

    public void writeLong(long l) throws IOException {
        if (useNio) {
            if (lastBuffer.capacity() < 8) {
                lastBuffer = ByteBuffer.allocateDirect(8);
            }
            lastBuffer.clear();
            lastBuffer.putLong(l);
            lastBuffer.flip();
            outputNio.write(lastBuffer);
        } else {
            CommonUtils.writeLong(outputBio, l);
        }
    }

    public void writeInt(int i) throws IOException {
        if (useNio) {
            if (lastBuffer.capacity() < 4) {
                lastBuffer = ByteBuffer.allocateDirect(4);
            }
            lastBuffer.clear();
            lastBuffer.putInt(i);
            lastBuffer.flip();
            outputNio.write(lastBuffer);
        } else {
            CommonUtils.writeInt(outputBio, i);
        }
    }

    public void writeDouble(double d) throws IOException {
        if (useNio) {
            if (lastBuffer.capacity() < 8) {
                lastBuffer = ByteBuffer.allocateDirect(8);
            }
            lastBuffer.clear();
            lastBuffer.putDouble(d);
            lastBuffer.flip();
            outputNio.write(lastBuffer);
        } else {
            CommonUtils.writeDouble(outputBio, d);
        }
    }

    public void writeString(ByteBuffer buffer) throws IOException {
        if (useNio) {
            lastBuffer.clear();
            lastBuffer.putInt(buffer.limit());
            lastBuffer.flip();
            outputNio.write(lastBuffer);
            outputNio.write(buffer);
        } else {
            CommonUtils.writeString(outputBio, buffer);
        }
    }

    public void flush() throws IOException {
        if (useNio) {
            outputNio.force(true);
        } else {
            outputBio.flush();
        }
    }

    public void close() throws IOException {
        if (useNio) {
            outputNio.close();
        } else {
            outputBio.close();
        }
    }
}
