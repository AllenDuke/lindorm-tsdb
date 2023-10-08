package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.util.NumberUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
    private long lastReadPos = -1;

    //    private DirectRandomAccessFile directRandomAccessFile;
    private MappedByteBuffer mappedByteBuffer;
    private long channelRealSize;

    private final int ioMode;

    protected boolean isDirty;

    private final File dataFile;
    private long size;

    public DataChannel(File dataFile, int ioMode, int nioBuffersSize, int bioBufferSize) throws IOException {
        this.ioMode = ioMode;
        this.dataFile = dataFile;
        inputRandomAccessFile = new RandomAccessFile(dataFile, "r");
        if (this.ioMode == 3) {
//            directRandomAccessFile = new DirectRandomAccessFile(dataFile, "rw");
//            lastBuffer = DirectIOUtils.allocateForDirectIO(directIOLib, BUFFER_SIZE);
            size = outputNio.size();
        } else if (this.ioMode == 2) {
            outputNio = new RandomAccessFile(dataFile, "rw").getChannel();
            size = outputNio.size();
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

    public int batchGzip(long batchPos, int batchSize) throws IOException {
        ByteBuffer read = this.read(batchPos, batchSize);
        size -= read.limit();
        byte[] bytes = gZip(read);
        size += bytes.length;
        outputNio.position(batchPos);
        outputNio.write(ByteBuffer.wrap(bytes));
        return bytes.length;
    }

    private byte[] gZip(ByteBuffer v) throws IOException {
        byte[] array1 = null;
        if (v.hasArray()) {
            array1 = v.array();
            if (array1.length != v.remaining()) {
                array1 = null;
            }
        }
        if (array1 == null) {
            array1 = new byte[v.remaining()];
            v.get(array1);
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(bos);
        byte[] b = null;
        gzip.write(array1);
        gzip.finish();
        b = bos.toByteArray();
        bos.close();
        gzip.close();
        return b;
    }

    /***
     * 解压GZip
     *
     * @param v
     * @return
     */
    public byte[] unGZip(ByteBuffer v) throws IOException {
        byte[] array1 = null;
        if (v.hasArray()) {
            array1 = v.array();
            if (array1.length != v.remaining()) {
                array1 = null;
            }
        }
        if (array1 == null) {
            array1 = new byte[v.remaining()];
            v.get(array1);
        }

        byte[] b = null;

        ByteArrayInputStream bis = new ByteArrayInputStream(array1);
        GZIPInputStream gzip = new GZIPInputStream(bis);
        byte[] buf = new byte[1024];
        int num = -1;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while ((num = gzip.read(buf, 0, buf.length)) != -1) {
            baos.write(buf, 0, num);
        }
        b = baos.toByteArray();
        baos.flush();
        baos.close();
        gzip.close();
        bis.close();
        return b;
    }

    private void nioFlushBuffer() throws IOException {
        lastBuffer.flip();
        int written = 0;
        while (written < lastBuffer.limit()) {
            if (ioMode == 2) {
                written += outputNio.write(lastBuffer, outputNio.position());
            }
            if (ioMode == 3) {
//                written += directRandomAccessFile.write(lastBuffer, outputNio.position());
            }
        }
        outputNio.position(outputNio.position() + written);
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
        if (ioMode == 3) {
            nioCheckAndFlushBuffer();
            lastBuffer.put(b);
        } else if (ioMode == 2) {
            nioCheckAndFlushBuffer();
            lastBuffer.put(b);
        } else {
            outputBio.write(b);
        }
    }

    private void writeBytes(byte[] b, int pos) throws IOException {
        if (ioMode == 3) {
            if (pos >= b.length) {
                return;
            }
            while (lastBuffer.hasRemaining() && pos < b.length) {
                lastBuffer.put(b[pos++]);
            }
            nioCheckAndFlushBuffer();
            // todo b太大可能会栈溢出，递归优化
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

    public void writeBytes(byte[] bytes) throws IOException {
        size += bytes.length;
        isDirty = true;
        if (lastReadPos >= 0) {
            lastBuffer.clear();
            lastReadPos = -1;
        }
        writeBytes(bytes, 0);
    }

    public void writeLong(long l) throws IOException {
        size += 8;
        isDirty = true;
        if (lastReadPos >= 0) {
            lastBuffer.clear();
            lastReadPos = -1;
        }
        if (ioMode == 3) {
            if (lastBuffer.remaining() >= 8) {
                lastBuffer.putLong(l);
            } else {
                byte[] b = new byte[8];
                UNSAFE.putLongUnaligned(b, ARRAY_BASE_OFFSET, l, true);
                writeBytes(b, 0);
            }
            nioCheckAndFlushBuffer();
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

    public void writeShort(short s) throws IOException {
        size += 2;
        isDirty = true;
        if (lastReadPos >= 0) {
            lastBuffer.clear();
            lastReadPos = -1;
        }
        if (ioMode == 3) {
            if (lastBuffer.remaining() >= 2) {
                lastBuffer.putShort(s);
            } else {
                byte[] b = new byte[2];
                UNSAFE.putIntUnaligned(b, ARRAY_BASE_OFFSET, s, true);
                writeBytes(b, 0);
            }
            nioCheckAndFlushBuffer();
        } else if (ioMode == 2) {
            if (lastBuffer.remaining() >= 2) {
                lastBuffer.putShort(s);
            } else {
                byte[] b = new byte[2];
                UNSAFE.putIntUnaligned(b, ARRAY_BASE_OFFSET, s, true);
                writeBytes(b, 0);
            }
            nioCheckAndFlushBuffer();
        } else {
            CommonUtils.writeShort(outputBio, s);
        }
    }

    public void writeInt(int i) throws IOException {
        size += 4;
        isDirty = true;
        if (lastReadPos >= 0) {
            lastBuffer.clear();
            lastReadPos = -1;
        }
        if (ioMode == 3) {
            if (lastBuffer.remaining() >= 4) {
                lastBuffer.putInt(i);
            } else {
                byte[] b = new byte[4];
                UNSAFE.putIntUnaligned(b, ARRAY_BASE_OFFSET, i, true);
                writeBytes(b, 0);
            }
            nioCheckAndFlushBuffer();
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
        if (lastReadPos >= 0) {
            lastBuffer.clear();
            lastReadPos = -1;
        }
        if (ioMode == 3) {
            if (lastBuffer.remaining() >= 8) {
                lastBuffer.putDouble(d);
                size += 8;
            } else {
                writeLong(Double.doubleToLongBits(d));
            }
            nioCheckAndFlushBuffer();
        } else if (ioMode == 2) {
            if (lastBuffer.remaining() >= 8) {
                lastBuffer.putDouble(d);
                size += 8;
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
        if (lastReadPos >= 0) {
            lastBuffer.clear();
            lastReadPos = -1;
        }
        if (ioMode == 3) {
            writeInt(buffer.limit());
            size += buffer.limit();
            writeBytes(buffer.array(), 0);
        } else if (ioMode == 2) {
            writeInt(buffer.limit());
            size += buffer.limit();
            writeBytes(buffer.array(), 0);
        } else {
            CommonUtils.writeString(outputBio, buffer);
        }
    }

    public long channelSize() throws IOException {
        if (ioMode == 3) {
            return size;
        } else if (ioMode == 2) {
            return size;
        } else {
            return 0;
        }
    }

    public void flush() throws IOException {
        if (!isDirty) {
            return;
        }
        if (ioMode == 3) {
            nioFlushBuffer();
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
//            directRandomAccessFile.close();
            return;
        }
        if (ioMode == 2) {
            if (size < outputNio.size()) {
                outputNio.truncate(size);
            }
            outputNio.close();
//            inputNio.close();
//            inputBioStream.close();
            inputRandomAccessFile.close();
        } else {
            outputBio.close();
        }
    }

    public InputStream readStream() throws FileNotFoundException {
        return new FileInputStream(dataFile);
    }

    protected ByteBuffer read(long pos, int size) throws IOException {
        if (ioMode == 3) {
            ByteBuffer allocate = ByteBuffer.allocate(size);
//            int read = directRandomAccessFile.read(allocate, pos);
//            allocate.limit(read);
            return allocate;
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

//        if (inputBioPos != pos) {
//            inputRandomAccessFile.seek(pos);
//            inputBioPos = pos;
//        }

        if (lastReadPos >= 0) {
            if (pos >= lastReadPos && pos + size <= lastReadPos + lastBuffer.limit()) {
                // 完整在读缓冲中
                lastBuffer.position((int) (pos - lastReadPos));
                ByteBuffer slice = lastBuffer.slice();
                slice.limit(size);
                return slice;
            }
        }
        ByteBuffer allocate;
        if (size <= lastBuffer.capacity()) {
            lastBuffer.clear();
            allocate = lastBuffer;
            lastReadPos = pos;
        } else {
            allocate = ByteBuffer.allocate(size);
        }

        int read = outputNio.read(allocate, pos);
        allocate.flip();

//        int read = inputRandomAccessFile.read(allocate.array());
//        allocate.limit(read);
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
        size += cnt;
        return cnt;
    }

    public final int writeZInt(int i) throws IOException {
        isDirty = true;
        if (lastReadPos >= 0) {
            lastBuffer.clear();
            lastReadPos = -1;
        }
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

    public double readZDouble(ByteBuffer buffer) throws IOException {
        int b = buffer.get() & 0xFF;
        if (b == 0xFF) {
            // 负数
            return Double.longBitsToDouble(buffer.getLong());
        } else if (b == 0xFE) {
            // 可以转换为float类型的
            return Float.intBitsToFloat(buffer.getInt());
        } else if ((b & 0x80) != 0) {
            // 范围在[-1..124]的小整数
            return (b & 0x7f) - 1;
        } else {
            // 负数double
            long bits =
                    ((long) b) << 56
                            | ((buffer.getInt() & 0xFFFFFFFFL) << 24)
                            | ((buffer.getShort() & 0xFFFFL) << 8)
                            | (buffer.get() & 0xFFL);
            return Double.longBitsToDouble(bits);
        }
    }

    public int writeZDouble(double d) throws IOException {
        isDirty = true;
        if (lastReadPos >= 0) {
            lastBuffer.clear();
            lastReadPos = -1;
        }
        int intVal = (int) d;
        final long doubleBits = Double.doubleToLongBits(d);
        if (d == intVal && intVal >= -1 && intVal <= 0x7C && doubleBits != Double.doubleToLongBits(-0d)) {
            // 第1种情况，可以用整数表示，且在[-1,124]: 单字节保存
            writeByte((byte) (0x80 | (intVal + 1)));
            size += 1;
            return 1;
        } else if (d == (float) d) {
            // 第2种情况，可以用浮点数保存，写入第一个字节0xFE作为标识符
            // 后边写入4个字节的float浮点数形式
            writeByte((byte) 0xFE);
            size += 1;
            writeInt(Float.floatToIntBits((float) d));
            return 5;
        } else if ((doubleBits >>> 63) == 0) {
            // 第3种情况，其他整数，需要8个字节
            writeByte((byte) (doubleBits >> 56));
            size += 1;
            writeInt((int) (doubleBits >>> 24));
            writeShort((short) (doubleBits >>> 8));
            writeByte((byte) (doubleBits));
            size += 1;
            return 8;
        } else {
            // 第4种情况，其他负数，需要9个字节
            writeByte((byte) 0xFF);
            size += 1;
            writeLong(doubleBits);
            return 9;
        }
    }
}
