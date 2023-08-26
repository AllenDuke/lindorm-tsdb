import com.alibaba.lindorm.contest.CommonUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class TestCommonUtils {
    public static void main(String[] args) throws IOException {
        TreeSet<Integer> set = new TreeSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(i);
        }
        SortedSet<Integer> tailSet = set.tailSet(53);
        tailSet.forEach(System.out::println);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        CommonUtils.writeInt(bout, 10);
        System.out.println(CommonUtils.readInt(new ByteArrayInputStream(bout.toByteArray())));

        bout = new ByteArrayOutputStream();
        CommonUtils.writeLong(bout, 20);
        System.out.println(CommonUtils.readLong(new ByteArrayInputStream(bout.toByteArray())));

        bout = new ByteArrayOutputStream();
        CommonUtils.writeDouble(bout, 30.10);
        System.out.println(CommonUtils.readDouble(new ByteArrayInputStream(bout.toByteArray())));

        byte[] strBuf = {1, 2, 3};
        ByteBuffer str = ByteBuffer.wrap(strBuf);
        bout = new ByteArrayOutputStream();
        CommonUtils.writeString(bout, str);
        ByteBuffer readBuf = CommonUtils.readString(new ByteArrayInputStream(bout.toByteArray()));
        System.out.println(readBuf.get());
        System.out.println(readBuf.get());
        System.out.println(readBuf.get());

        strBuf = new byte[0];
        str = ByteBuffer.wrap(strBuf);
        bout = new ByteArrayOutputStream();
        CommonUtils.writeString(bout, str);
        readBuf = CommonUtils.readString(new ByteArrayInputStream(bout.toByteArray()));
        System.out.println(readBuf.remaining());
    }
}
