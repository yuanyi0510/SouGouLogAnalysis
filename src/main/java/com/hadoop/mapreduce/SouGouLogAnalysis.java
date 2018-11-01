package com.hadoop.mapreduce;

import org.apache.hadoop.util.ToolRunner;

public class SouGouLogAnalysis {
    public static void main(String[] args) throws Exception {
        int statuscount = ToolRunner.run(new WordCountMapReduce(), new String[]{args[0], args[1]});
        if (statuscount != -1) {
            int statussort = ToolRunner.run(new SortKeyWordMapReduce(), new String[]{args[1], args[2]});
            System.exit(statussort);
        } else {
            System.exit(statuscount);
        }
    }
}
