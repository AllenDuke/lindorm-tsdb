import com.alibaba.lindorm.contest.CommonUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TestCommonUtils {
    public static void main(String[] args) throws IOException {
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
