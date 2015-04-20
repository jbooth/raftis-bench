package raftisbench;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 */
public class Util {
    public static String report(List<SummaryStatistics> in) {
        long totalEvents = 0;
        double totalTime = 0.0d;
        for (SummaryStatistics s : in) {
            totalEvents += s.getN();
            totalTime += (double)s.getN() * s.getMean();
        }
        double weightedAvg = 0.0d;
        for (SummaryStatistics s : in) {
            weightedAvg = s.getMean() * ((double)s.getN() / totalEvents);
        }
        return "Total Records: " + totalEvents + ", avg subjective latency: " + weightedAvg + ", total system throughput: records/sec: " + ((double)totalEvents / (totalTime / 1e6));
    }

    public static <T> List<T> runParallel(List<Callable<T>> in) throws ExecutionException, InterruptedException {
        ExecutorService exec = Executors.newFixedThreadPool(in.size());
        List<Future<T>> waiting = new ArrayList<Future<T>>();
        for (Callable<T> c : in) {
            waiting.add(exec.submit(c));
        }
        List<T> ret = new ArrayList<T>();
        for (Future<T> f : waiting) {
            ret.add(f.get());
        }
        return ret;
    }
}
