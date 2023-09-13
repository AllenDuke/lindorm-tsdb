package com.alibaba.lindorm.contest;

import jdk.internal.misc.Unsafe;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CommonUtils {
    // Add "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED" to your VM properties to enable unsafe.
    private static final Unsafe UNSAFE = Unsafe.getUnsafe();
    private static final long ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    public static void writeLong(OutputStream out, long v) throws IOException {
        byte[] b = new byte[8];
        UNSAFE.putLongUnaligned(b, ARRAY_BASE_OFFSET, v, true);
        out.write(b);
    }

    public static void writeInt(OutputStream out, int v) throws IOException {
        byte[] b = new byte[4];
        UNSAFE.putIntUnaligned(b, ARRAY_BASE_OFFSET, v, true);
        out.write(b);
    }

    public static void writeShort(OutputStream out, short v) throws IOException {
        byte[] b = new byte[2];
        UNSAFE.putShortUnaligned(b, ARRAY_BASE_OFFSET, v, true);
        out.write(b);
    }

    public static void writeDouble(OutputStream out, double v) throws IOException {
        writeLong(out, Double.doubleToLongBits(v));
    }

    /***
     * 压缩GZip
     *
     * @param data
     * @return
     */
    public static byte[] gZip(byte[] data) {
        byte[] b = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(bos);
            gzip.write(data);
            gzip.finish();
            gzip.close();
            b = bos.toByteArray();
            bos.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return b;
    }

    /***
     * 解压GZip
     *
     * @param data
     * @return
     */
    public static byte[] unGZip(byte[] data) {
        byte[] b = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
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
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return b;
    }

    public static void writeString(OutputStream out, ByteBuffer v) throws IOException {
        int zip = v.remaining();
//        if (v.remaining() > 0) {
//            byte[] gZip = gZip(v.array());
//            if (gZip.length < v.remaining()) {
//                v = ByteBuffer.wrap(gZip);
//                zip = -v.remaining();
//            }
//        }

        writeInt(out, zip);
        if (v.remaining() > 0) {
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
            out.write(array1);
        }
    }

    public static long readLong(InputStream in) throws IOException {
        byte[] b = new byte[8];
        int ret = in.readNBytes(b, 0, 8);
        if (ret != 8) {
            throw new EOFException();
        }
        return UNSAFE.getLongUnaligned(b, ARRAY_BASE_OFFSET, true);
    }

    public static int readInt(InputStream in) throws IOException {
        byte[] b = new byte[4];
        int ret = in.readNBytes(b, 0, 4);
        if (ret != 4) {
            throw new EOFException();
        }
        return UNSAFE.getIntUnaligned(b, ARRAY_BASE_OFFSET, true);
    }

    public static double readDouble(InputStream in) throws IOException {
        return Double.longBitsToDouble(readLong(in));
    }

    public static ByteBuffer readString(InputStream in) throws IOException {
        int strLen = readInt(in);
        if (strLen == 0) {
            ByteBuffer res = ByteBuffer.allocate(0);
            res.flip();
            return res;
        }

        boolean zip = false;
        if (strLen < 0) {
            zip = true;
            strLen = -strLen;
        }
        byte[] b = new byte[strLen];
        int ret = in.readNBytes(b, 0, strLen);
        if (ret != strLen) {
            throw new EOFException();
        }

        if (zip) {
            return ByteBuffer.wrap(unGZip(b));
        } else {
            return ByteBuffer.wrap(b);
        }

    }

    public static boolean cleanDir(File dir, boolean deleteDirItself) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            assert children != null;
            for (String child : children) {
                boolean ret = cleanDir(new File(dir, child), true);
                if (!ret) {
                    return false;
                }
            }
        }
        if (deleteDirItself) {
            return dir.delete();
        }
        return true;
    }
}
