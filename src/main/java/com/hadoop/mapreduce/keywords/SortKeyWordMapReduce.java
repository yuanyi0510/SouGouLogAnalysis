package com.hadoop.mapreduce.keywords;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;

public class SortKeyWordMapReduce extends Configured implements Tool {
    //step 1: Map Class

    /**
     * 输入：<偏移量，词频+关键词>
     * 输出：<词频，关键词>
     */
    public static class SortKeyWordMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable mapOutputKey = new LongWritable();
        private Text mapOutputValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // TODO
            String str[] = value.toString().split("\t");
            long fre=Long.parseLong(str[0]);
            mapOutputKey.set(fre);
            mapOutputValue.set(str[1]);
            context.write(mapOutputKey, mapOutputValue);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }


    /**
     * 输入：<词频，关键词>
     * 输出：<词频，关键词>
     */
    //step 2: Reduce Class
    public static class SortKeyWordReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private final static int TOP_THRITY = 30;
        private int time = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //TODO
            Iterator<Text> var4 = values.iterator();
            while (var4.hasNext()) {
                Text value = var4.next();
                context.write(key, value);
            }

        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            this.setup(context);
            try {
                while (context.nextKey()) {
                    if (time < 30) {
                        time++;
                        this.reduce(context.getCurrentKey(), context.getValues(), context);
                        Iterator<Text> iterator = context.getValues().iterator();
                        if (iterator instanceof ReduceContext.ValueIterator) {
                            ((ReduceContext.ValueIterator) iterator).resetBackupStore();
                        }
                    } else {
                        break;
                    }
                }
            } finally {
                this.cleanup(context);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

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
        //4.submit job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }
}
