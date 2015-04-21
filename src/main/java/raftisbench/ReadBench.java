package raftisbench;

import com.volkangurel.raftis.RaftisClient;
import com.volkangurel.raftis.RaftisClientImpl;
import com.volkangurel.raftis.config.RaftisPoolConfig;
import com.volkangurel.raftis.config.RaftisShardHostConfig;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Created by jay on 4/19/15.
 */
public class ReadBench {


    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String group = args[2];
        int spaceSize = Integer.parseInt(args[3]);
        int readsPerThread = Integer.parseInt(args[4]) * (int)1e3;
        int numThreads = Integer.parseInt(args[5]);

        RaftisPoolConfig poolConfig = new RaftisPoolConfig()
                .setMaxIdle(10)
                .setTimeout(1000);

        RaftisClientImpl cli = new RaftisClientImpl.Builder(poolConfig, group).addSeed(new RaftisShardHostConfig.Builder(host,group).port(port).build()).build();
        System.out.println("WriteBenchPooled, launching " + numThreads + " threads to insert " + readsPerThread + " total entries.");
        List<Callable<SummaryStatistics>> tasks = new ArrayList<Callable<SummaryStatistics>>();
        for (int i = 0 ; i < numThreads ; i++) {
            tasks.add(new ReadThread(spaceSize,readsPerThread,cli));
        }
        System.out.println(Util.report(Util.runParallel(tasks)));
    }

    public static class ReadThread implements Callable<SummaryStatistics> {
        private final int entrySpaceSize;
        private final int numReads;
        private final RaftisClient cli;

        public ReadThread(int entrySpaceSize, int numReads, RaftisClient cli) {
            this.entrySpaceSize = entrySpaceSize;
            this.numReads = numReads;
            this.cli = cli;
        }

        @Override
        public SummaryStatistics call() throws Exception {
            System.out.println("Thread doing " + numReads + " reads");
            SummaryStatistics stats = new SummaryStatistics();
            Random r = new Random();
            for (int i = 0 ; i < numReads; i++) {
                Data.KV lookup = Data.entry(r.nextInt(entrySpaceSize)); // random key from our space
                long start = System.nanoTime();
                String val = cli.get(lookup.k);
                long elapsed = System.nanoTime() - start;
                System.out.println("Did a read in " + elapsed + " nanos");
                stats.addValue(elapsed / 1e6); // store value in millis
            }
            return stats;
        }




    }
}
