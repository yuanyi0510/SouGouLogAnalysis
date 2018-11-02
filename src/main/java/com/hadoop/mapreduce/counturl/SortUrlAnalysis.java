package com.hadoop.mapreduce.counturl;

import org.apache.hadoop.util.ToolRunner;

public class SortUrlAnalysis {
    public static void main(String[] args) throws Exception {
        UrlCountMapReduce urlCountMapReduce = new UrlCountMapReduce();
        int statuscount = ToolRunner.run(urlCountMapReduce, new String[]{args[0], args[1]});
        if (statuscount != -1) {
            int statussort = ToolRunner.run(new SortUrlMapReduce(urlCountMapReduce.getTotal()),
                    new String[]{args[1], args[2]});
            System.exit(statussort);
        } else {
            System.exit(statuscount);
        }
    }
}
