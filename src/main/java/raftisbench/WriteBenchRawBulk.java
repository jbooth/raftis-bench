package raftisbench;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;

/**
 * Created by jay on 4/19/15.
 */
public class WriteBenchRawBulk {
    public static void main(String[] args) throws Exception {
        String redisAddr = args[0];
        int redisPort = Integer.parseInt(args[1]);
        int numEntries = Integer.parseInt(args[2]);

        Socket s = new Socket(redisAddr,redisPort);

        long start = System.nanoTime();
        Thread writes = new Thread(
                new DoWrites(numEntries,s.getOutputStream()));
        Thread acks = new Thread(new AckWrites(numEntries, s.getInputStream()));

        writes.start();
        acks.start();
        writes.join();
        acks.join();
        long elapsed = System.nanoTime() - start;
        System.out.println("Wrote " + numEntries + " 128B keys, 2048B values in " + elapsed + " nanos");
        System.out.println("Records per second: " + ((double)numEntries / ((double)elapsed * 1e9)));
    }

    private static class DoWrites implements Runnable {
        private final int numRecords;
        private final OutputStream out;
        private final Data data = new Data();

        public DoWrites(int numRecords, OutputStream out) {
            this.numRecords = numRecords;
            this.out = out;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < numRecords; i++) {
                    Data.KV kv = Data.entry(i);
                    String command =
                            "*3\r\n" +
                                    "$3\r\nSET\r\n" +
                                    "$" + kv.k.length() + "\r\n" + kv.k + "\r\n" +
                                    "$" + kv.v.length() + "\r\n" + kv.v + "\r\n";
                    out.write(command.getBytes());
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            } finally {

                try {
                    System.out.println("Wrote " + numRecords + " SET commands, closing stream..");
                    out.close();
                } catch (Exception e) {e.printStackTrace();}

            }
        }
    }

    private static class AckWrites implements Runnable {
        private final int numRecords;
        private static final byte[] OK = new byte[]{'+','O','K','\r','\n'};
        private final InputStream in;
        private AckWrites(int numRecords, InputStream in) {
            this.numRecords = numRecords;
            this.in = new BufferedInputStream(in);
        }

        @Override
        public void run() {
            byte[] ack = new byte[5]; // long enough for +OK\r\n
            try {
                for (int i = 0 ; i < numRecords ; i++) {
                    in.read(ack);
                    if (!Arrays.equals(ack,OK)) {
                        throw new RuntimeException("Got wrong response!  Wanted +OK\\r\\n, got " + new String(ack));
                    }
                }
            } catch (Throwable ioe) {
                ioe.printStackTrace();
                System.exit(1);
            } finally {
                try {
                    in.close();
                } catch (IOException e) {e.printStackTrace();}
            }
        }

    }
}
