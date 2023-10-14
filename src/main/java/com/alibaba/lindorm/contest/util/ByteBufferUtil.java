package com.alibaba.lindorm.contest.util;

import com.github.luben.zstd.Zstd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ByteBufferUtil {

    public static byte[] toBytes(ByteBuffer v) {
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
        return array1;
    }

    public static byte[] gZip(byte[] array1) throws IOException {
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

    public static byte[] gZip(ByteBuffer v) throws IOException {
        byte[] array1 = ByteBufferUtil.toBytes(v);
        return gZip(array1);
    }

    public static byte[] unGZip(byte[] array1) throws IOException {
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

    public static byte[] unGZip(ByteBuffer v) throws IOException {
        byte[] array1 = ByteBufferUtil.toBytes(v);
        return unGZip(array1);
    }


    public static byte[] zstdEncode(byte[] array1) throws IOException {
        return Zstd.compress(array1);
    }

    //    7643 610360
    public static byte[] zstdDecode(ByteBuffer v) throws IOException {
        byte[] array1 = ByteBufferUtil.toBytes(v);
        int size = (int) Zstd.decompressedSize(array1);
        return Zstd.decompress(array1, size);
    }
}
