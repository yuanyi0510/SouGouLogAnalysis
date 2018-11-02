package com.hadoop.mapreduce.counturl;

import org.apache.hadoop.util.ToolRunner;

public class SortUrlAnalysis {
    public static void main(String[] args) throws Exception {
        int statuscount = ToolRunner.run(new UrlCountMapReduce(), new String[]{args[0], args[1]});
        if (statuscount != -1) {
            int statussort = ToolRunner.run(new SortUrlMapReduce(SortUrlMapReduce.SortUrlReducer.getTOTAL()),
                    new String[]{args[1], args[2]});
            System.exit(statussort);
        } else {
            System.exit(statuscount);
        }
    }
}
