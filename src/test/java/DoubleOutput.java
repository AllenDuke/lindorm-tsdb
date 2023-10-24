import java.math.BigDecimal;
import java.util.Random;

public class DoubleOutput {

    public static void main(String[] args) {
        double v = 20155.045719311995;
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            System.out.println(v);
            v = 20155.045719311995 + random.nextDouble() * 0.001;
            String s = String.valueOf(v);
            System.out.println(s.substring(0, Math.min(18, s.length())));
        }
    }
}
