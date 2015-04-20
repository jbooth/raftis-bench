package raftisbench;

import org.apache.commons.codec.binary.Hex;

import java.util.Random;

/**
 * Maintains consistent random data -- if you want a dataset of 100 items, work with the items that come back from entry(1..100)
 */
public class Data {

    public static class KV {
        public String k;
        public String v;
    }

    public static KV entry(int entryCount) {
        Random r = new Random(entryCount);
        byte[] k = new byte[64];
        byte[] v = new byte[1024];
        r.nextBytes(k);
        r.nextBytes(v);
        KV ret = new KV();
        ret.k = bytesToHex(k);
        ret.v = bytesToHex(v);
        return ret;
    }

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
