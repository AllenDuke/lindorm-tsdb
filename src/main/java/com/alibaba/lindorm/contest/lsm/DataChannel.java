package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibaba.lindorm.contest.CommonUtils.ARRAY_BASE_OFFSET;
import static com.alibaba.lindorm.contest.CommonUtils.UNSAFE;

public class DataChannel {

    private FileChannel outputNio;

    private OutputStream outputBio;

    private ByteBuffer lastBuffer;

    private final int ioMode;

    public DataChannel(File dataFile, int ioMode, int nioBuffersSize, int bioBufferSize) throws FileNotFoundException {
        this.ioMode = ioMode;
        if (this.ioMode == 2) {
            outputNio = new FileOutputStream(dataFile, true).getChannel();
            lastBuffer = ByteBuffer.allocateDirect(Math.max(nioBuffersSize / 1024 + 1024, 16 * 1024));
        } else if (this.ioMode == 1) {
            outputBio = new BufferedOutputStream(new FileOutputStream(dataFile, true), bioBufferSize);
        } else {
            outputBio = new FileOutputStream(dataFile, true);
        }
    }

    private void nioFlushBuffer() throws IOException {
        lastBuffer.flip();
        int written = 0;
        while (written < lastBuffer.limit()) {
            written += outputNio.write(lastBuffer);
        }
        lastBuffer.clear();
    }

    private void nioCheckAndFlushBuffer() throws IOException {
        if (lastBuffer.position() < lastBuffer.limit()) {
            // 1024字节对齐写入效率更高
            return;
        }

        nioFlushBuffer();
    }

    private void writeBytes(byte[] b, int pos) throws IOException {
        if (ioMode == 2) {
            if (pos >= b.length) {
                return;
            }
            while (lastBuffer.hasRemaining() && pos < b.length) {
                lastBuffer.put(b[pos++]);
            }
            nioCheckAndFlushBuffer();
            // todo b太大可能会栈溢出，递归优化
            writeBytes(b, pos);
        } else {
            outputBio.write(b);
        }
    }

    public void writeLong(long l) throws IOException {
        if (ioMode == 2) {
            if (lastBuffer.remaining() >= 8) {
                lastBuffer.putLong(l);
            } else {
                byte[] b = new byte[8];
                UNSAFE.putLongUnaligned(b, ARRAY_BASE_OFFSET, l, true);
                writeBytes(b, 0);
            }
            nioCheckAndFlushBuffer();
        } else {
            CommonUtils.writeLong(outputBio, l);
        }
    }

    public void writeInt(int i) throws IOException {
        if (ioMode == 2) {
            if (lastBuffer.remaining() >= 4) {
                lastBuffer.putInt(i);
            } else {
                byte[] b = new byte[4];
                UNSAFE.putIntUnaligned(b, ARRAY_BASE_OFFSET, i, true);
                writeBytes(b, 0);
            }
            nioCheckAndFlushBuffer();
        } else {
            CommonUtils.writeInt(outputBio, i);
        }
    }

    public void writeDouble(double d) throws IOException {
        if (ioMode == 2) {
            if (lastBuffer.remaining() >= 8) {
                lastBuffer.putDouble(d);
            } else {
                writeLong(Double.doubleToLongBits(d));
            }
            nioCheckAndFlushBuffer();
        } else {
            CommonUtils.writeDouble(outputBio, d);
        }
    }

    public void writeString(ByteBuffer buffer) throws IOException {
        if (ioMode == 2) {
            writeInt(buffer.limit());
            writeBytes(buffer.array(), 0);
        } else {
            CommonUtils.writeString(outputBio, buffer);
        }
    }

    public void flush() throws IOException {
        if (ioMode == 2) {
            nioFlushBuffer();
            outputNio.force(true);
        } else {
            outputBio.flush();
        }
    }

    public void close() throws IOException {
        if (ioMode == 2) {
            outputNio.close();
        } else {
            outputBio.close();
        }
    }
}
