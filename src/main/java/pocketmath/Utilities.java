package pocketmath;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public final class Utilities {

    // MessageDigest didn't specify anything about the thread safety of object obtained via getInstance() call
    // and the nature of the api does imply MessageDigest is mutable, so let's just be kiasu here
    private static final ThreadLocal<MessageDigest> threadLocalMessageDigest = new ThreadLocal<MessageDigest>() {
        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("not possible");
            }
        }
    };

    public static byte[] md5(String text) {
        MessageDigest md = threadLocalMessageDigest.get();
        md.reset();
        return md.digest(text.getBytes(StandardCharsets.UTF_8));
    }

    public static long currentHourEpoch() {
        return hourEpochOf(System.currentTimeMillis());
    }

    public static long hourEpochOf(long millisEpoch) {
        return millisEpoch / TimeUnit.HOURS.toMillis(1);
    }

    // https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
    public static String mysqlHex(byte[] data) {
        return "0x" + DatatypeConverter.printHexBinary(data);
    }

    // http://stackoverflow.com/a/4555351/221965
    public static <T extends Throwable> RuntimeException up(Throwable throwable) throws T {
        throw (T) throwable;
    }
}
