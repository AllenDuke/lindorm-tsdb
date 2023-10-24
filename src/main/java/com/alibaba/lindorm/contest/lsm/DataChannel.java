package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.elf.ElfOnChimpCompressor;
import com.alibaba.lindorm.contest.elf.ElfOnChimpDecompressor;
import com.alibaba.lindorm.contest.elf.ICompressor;
import com.alibaba.lindorm.contest.elf.IDecompressor;
import com.alibaba.lindorm.contest.util.ByteBufferUtil;
import com.alibaba.lindorm.contest.util.NumberUtil;
import com.github.luben.zstd.Zstd;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.alibaba.lindorm.contest.CommonUtils.ARRAY_BASE_OFFSET;
import static com.alibaba.lindorm.contest.CommonUtils.UNSAFE;

public class DataChannel {

    public static final AtomicLong LAST_CNT = new AtomicLong();
    public static final AtomicLong LAST_HALF_CNT = new AtomicLong();

    public static final Map<Thread, Integer> MAX_SCALE_MAP = new ConcurrentHashMap<>();

    public static final int BUFFER_SIZE = 16 * 1024;

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
//            outputNio.force(true);
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
            if (outputNio.isOpen() && size < outputNio.size()) {
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

    protected ByteBuffer read(long pos, int size) throws IOException {
        if (ioMode == 3) {
            ByteBuffer allocate = ByteBuffer.allocate(size);
//            int read = directRandomAccessFile.read(allocate, pos);
//            allocate.limit(read);
            return allocate;
        }

//        if (ioMode == 2) {
        MappedByteBuffer map = outputNio.map(FileChannel.MapMode.READ_ONLY, pos, Math.min(size, outputNio.size() - pos));
        return map.load();
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

//        if (lastReadPos >= 0) {
//            LAST_CNT.incrementAndGet();
//            if (pos >= lastReadPos && pos + size <= lastReadPos + lastBuffer.limit()) {
//                // 完整在读缓冲中
//                lastBuffer.position((int) (pos - lastReadPos));
//                ByteBuffer slice = lastBuffer.slice();
//                slice.limit(size);
//                return slice;
//            } else if (pos >= lastReadPos && pos <= lastReadPos + lastBuffer.limit()) {
//                LAST_HALF_CNT.incrementAndGet();
//            }
//        }
//        ByteBuffer allocate;
////        if (size <= lastBuffer.capacity()) {
////            lastBuffer.clear();
////            allocate = lastBuffer;
////            lastReadPos = pos;
////        } else {
//            allocate = ByteBuffer.allocate(size);
////        }
//
//        int read = outputNio.read(allocate, pos);
//        allocate.flip();
//        allocate = allocate.slice();
//        allocate.limit(Math.min(read, size));
//
////        int read = inputRandomAccessFile.read(allocate.array());
////        allocate.limit(read);
//        inputBioPos += read;
//        return allocate;
    }
//    }
}
