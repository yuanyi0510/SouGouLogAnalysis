package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.*;

public class SortKeyWordMapReduce extends Configured implements Tool {
    //step 1: Map Class

    /**
     * 输入：<词频，关键词>
     * 输出：<词频，关键词>
     */
    public static class SortKeyWordMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private Text mapOutputKey = new Text();
        private final static IntWritable mapOutputValue = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // TODO
            super.map(key, value, context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //super.cleanup(context);
        }
    }


    /**
     * 输入：<词频，关键词>
     * 输出：<词频，关键词>
     */
    //step 2: Reduce Class
    public static class SortKeyWordReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private final static LongWritable outputValue = new LongWritable();
        List<String> sortList = new LinkedList<String>();
        Map<String, Long> map = new HashMap<String, Long>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //  super.setup(context);
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //TODO
           /* Iterator<Text> var4=values.iterator();
            while (var4.hasNext()){
                Text text=var4.next();
                context.write(key,text);
            }*/
            for (Text value : values) {
                String line = value.toString();
                map.put(line, key.get());
            }
            List<Map.Entry<String, Long>> list =
                    new LinkedList<Map.Entry<String, Long>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return (int) (o1.getValue()-o2.getValue());
                }
            });
            for (int i=0;i<30;i++){
                Map.Entry<String,Long> mapping=list.get(i);
                sortList.add(mapping.getKey());
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // super.cleanup(context);
            int len=sortList.size();
            for (String str:sortList){
                context.write(context.getCurrentKey(),new Text(str));
            }
        }
    }

    //step 3: Driver ,component job
    public int run(String[] args) throws Exception {
        //1.get configuration
        Configuration configuration = getConf();
        //2.create Job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        //3.set Job
        //3.1 input
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        //3.2 map
        job.setMapperClass(SortKeyWordMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setSortComparatorClass(SortKeyWordComparator.class);
        //3.3 reduce
        job.setReducerClass(SortKeyWordReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        //3.4 output
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);
        job.setSortComparatorClass(SortKeyWordComparator.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        //4.submit job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }
}
