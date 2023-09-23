package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.util.NumberUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.alibaba.lindorm.contest.CommonUtils.ARRAY_BASE_OFFSET;
import static com.alibaba.lindorm.contest.CommonUtils.UNSAFE;

public class DataChannel {

    private static final int BUFFER_SIZE = 16 * 1024;

    private FileChannel outputNio;

    private OutputStream outputBio;

    //    private InputStream inputBioStream;
    private RandomAccessFile inputRandomAccessFile;

//    private FileChannel inputNio;

    private long inputBioPos;

    private ByteBuffer lastBuffer;

    private FileChannel channel;
    private MappedByteBuffer mappedByteBuffer;
    private long channelRealSize;

    private final int ioMode;

    private boolean isDirty;

//    private final File dataFile;

    public DataChannel(File dataFile, int ioMode, int nioBuffersSize, int bioBufferSize) throws IOException {
        this.ioMode = ioMode;
//        this.dataFile = dataFile;
        inputRandomAccessFile = new RandomAccessFile(dataFile, "r");
        if (this.ioMode == 3) {
            channel = new RandomAccessFile(dataFile, "rw").getChannel();
            mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 8);
            channelRealSize = mappedByteBuffer.getLong();
            if (channelRealSize > 0) {
                mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 8, BUFFER_SIZE);
            }
        } else if (this.ioMode == 2) {
            outputNio = new FileOutputStream(dataFile, true).getChannel();
//            inputNio = new FileInputStream(dataFile).getChannel();
//            inputBioStream = new FileInputStream(dataFile); 

            // 5000个vin 60+1列，这里需要2.5GB todo 池化 资源管理 flush的时候归还
            lastBuffer = ByteBuffer.allocateDirect(Math.max(nioBuffersSize / 1024 + 1024, BUFFER_SIZE));
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

    private void writeByte(byte b) throws IOException {
        isDirty = true;
        if (ioMode == 3) {
            if (!mappedByteBuffer.hasRemaining()) {
                // 当前已写满，向下增长
                channelRealSize += BUFFER_SIZE;
                mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 8 + channelRealSize, BUFFER_SIZE);
            }
            mappedByteBuffer.put(b);
        } else if (ioMode == 2) {
            nioCheckAndFlushBuffer();
            lastBuffer.put(b);
        } else {
            outputBio.write(b);
        }
    }

    private void writeBytes(byte[] b, int pos) throws IOException {
        isDirty = true;
        if (ioMode == 3) {
            if (pos >= b.length) {
                return;
            }
            while (mappedByteBuffer.hasRemaining() && pos < b.length) {
                mappedByteBuffer.put(b[pos++]);
            }
            if (mappedByteBuffer.position() >= mappedByteBuffer.limit()) {
                // 当前已写满，向下增长
                channelRealSize += BUFFER_SIZE;
                mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 8 + channelRealSize, BUFFER_SIZE);
            }

            writeBytes(b, pos);
        } else if (ioMode == 2) {
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
        isDirty = true;
        if (ioMode == 3) {
            if (mappedByteBuffer.remaining() >= 8) {
                mappedByteBuffer.putLong(l);
            } else {
                byte[] b = new byte[8];
                UNSAFE.putLongUnaligned(b, ARRAY_BASE_OFFSET, l, true);
                writeBytes(b, 0);
            }
        } else if (ioMode == 2) {
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
        isDirty = true;
        if (ioMode == 3) {
            if (mappedByteBuffer.remaining() >= 4) {
                mappedByteBuffer.putInt(i);
            } else {
                byte[] b = new byte[4];
                UNSAFE.putIntUnaligned(b, ARRAY_BASE_OFFSET, i, true);
                writeBytes(b, 0);
            }
        } else if (ioMode == 2) {
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
        isDirty = true;
        if (ioMode == 3) {
            if (mappedByteBuffer.remaining() >= 8) {
                mappedByteBuffer.putDouble(d);
            } else {
                writeLong(Double.doubleToLongBits(d));
            }
        } else if (ioMode == 2) {
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
        isDirty = true;
        if (ioMode == 3) {
            writeInt(buffer.limit());
            writeBytes(buffer.array(), 0);
        } else if (ioMode == 2) {
            writeInt(buffer.limit());
            writeBytes(buffer.array(), 0);
        } else {
            CommonUtils.writeString(outputBio, buffer);
        }
    }

    public void flush() throws IOException {
        if (!isDirty) {
            return;
        }
        if (ioMode == 3) {

        } else if (ioMode == 2) {
            nioFlushBuffer();
            outputNio.force(true);
        } else {
            outputBio.flush();
        }
        isDirty = false;
    }

    public void close() throws IOException {
        if (ioMode == 3) {
//            channel.truncate(channelRealSize + 8);
            channel.close();
            return;
        }
        if (ioMode == 2) {
            outputNio.close();
//            inputNio.close();
//            inputBioStream.close();
            inputRandomAccessFile.close();
        } else {
            outputBio.close();
        }
    }

    protected ByteBuffer read(long pos, int size) throws IOException {
        if (ioMode == 3) {
            pos += 8;
            return channel.map(FileChannel.MapMode.READ_ONLY, pos, Math.min(size, channel.size() - pos));
        }

//        if (ioMode == 2) {
//            return inputNio.map(FileChannel.MapMode.READ_ONLY, pos, Math.min(size, outputNio.size() - pos));
//        } else {
//        if (inputBioPos != pos) {
//            inputBioStream.close();
//            inputBioStream = new FileInputStream(dataFile);
//            inputBioStream.skip(pos);
//            inputBioPos = pos;
//        }
//        byte[] bytes = inputBioStream.readNBytes(size);
//        inputBioPos += bytes.length;
//        return ByteBuffer.wrap(bytes);

        if (inputBioPos != pos) {
            inputRandomAccessFile.seek(pos);
            inputBioPos = pos;
        }

        ByteBuffer allocate = ByteBuffer.allocate(size);
        int read = inputRandomAccessFile.read(allocate.array());
        allocate.limit(read);
        inputBioPos += read;
        return allocate;
    }
//    }

    private int writeVInt(int i) throws IOException {
        int cnt = 0;
        while ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7F) | 0x80));
            cnt++;
            i >>>= 7;
        }
        writeByte((byte) i);
        cnt++;
        return cnt;
    }

    public final int writeZInt(int i) throws IOException {
        return writeVInt(NumberUtil.zigZagEncode(i));
    }

    private int readVInt(ByteBuffer buffer) throws IOException {
        byte b = buffer.get();
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = buffer.get();
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = buffer.get();
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = buffer.get();
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = buffer.get();
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
        throw new IOException("Invalid vInt detected (too many bits)");
    }

    public int readZInt(ByteBuffer buffer) throws IOException {
        return NumberUtil.zigZagDecode(readVInt(buffer));
    }
}
