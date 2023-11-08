import com.alibaba.lindorm.contest.util.ByteBufferUtil;
import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class TestZstd {

    public static void main(String[] args) {
        int cnt = 1024;
        ByteBuffer allocate = ByteBuffer.allocate(cnt * 4 + cnt * 10);
        Random random = new Random();
        for (int i = 0; i < cnt; i++) {
            if (random.nextDouble()<0.5) {
                allocate.putInt(7);
                allocate.put("SUCCESS".getBytes(StandardCharsets.UTF_8));
            }else{
                allocate.putInt(6);
                allocate.put("FAILED".getBytes(StandardCharsets.UTF_8));
            }
        }
        allocate.flip();
        byte[] bytes = ByteBufferUtil.toBytes(allocate);
        byte[] compress = Zstd.compress(bytes);
        byte[] compress1 = Zstd.compress(bytes, 10);
        System.out.println(bytes.length + " " + compress.length + " " + compress1.length);
    }
}
