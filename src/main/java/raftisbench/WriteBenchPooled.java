package raftisbench;

import com.volkangurel.raftis.RaftisClient;
import com.volkangurel.raftis.RaftisClientImpl;
import com.volkangurel.raftis.config.RaftisPoolConfig;
import com.volkangurel.raftis.config.RaftisShardHostConfig;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by jay on 4/19/15.
 */
public class WriteBenchPooled {

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String group = args[2];
        int numEntries = Integer.parseInt(args[3]) * (int)1e6;
        int numThreads = Integer.parseInt(args[4]);

        RaftisPoolConfig poolConfig = new RaftisPoolConfig()
                .setMaxIdle(10)
                .setTimeout(1000);

        RaftisClientImpl cli = new RaftisClientImpl.Builder(poolConfig, group).addSeed(new RaftisShardHostConfig.Builder(host,group).port(port).build()).build();
        System.out.println("WriteBenchPooled, launching " + numThreads + " threads to insert " + numEntries + " total entries.");
        List<Callable<SummaryStatistics>> tasks = new ArrayList<Callable<SummaryStatistics>>();
        for (int i = 0 ; i < numThreads ; i++) {
            tasks.add(new Writer(numEntries,i, numThreads, cli));
        }
        System.out.println(Util.report(Util.runParallel(tasks)));
    }

    private static class Writer implements Callable<SummaryStatistics> {
        private final int numTotalEntries;
        private final int myStripe;
        private final int numThreads;
        private final RaftisClient cli;


        private Writer(int numTotalEntries, int myStripe, int numThreads, RaftisClient cli) {
            this.numTotalEntries = numTotalEntries;
            this.myStripe = myStripe;
            this.numThreads = numThreads;
            this.cli = cli;
        }

        @Override
        public SummaryStatistics call() throws Exception {
            SummaryStatistics ret = new SummaryStatistics();
            for (int i = 0 ; i < numTotalEntries ; i++) {
                if (i % numThreads == myStripe) {
                    Data.KV toPut = Data.entry(i);
                    long start = System.nanoTime();
                    cli.set(toPut.k, toPut.v);
                    long elapsed = System.nanoTime();
                    ret.addValue(elapsed * 1e6); // millis
                }
            }
            return ret;

        }
    }
}
