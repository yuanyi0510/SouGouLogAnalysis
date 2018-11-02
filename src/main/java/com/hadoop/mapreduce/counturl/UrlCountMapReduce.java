package com.hadoop.mapreduce.counturl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class UrlCountMapReduce extends Configured implements Tool {
    //step 1: Map Class

    /**
     * 获取搜索词
     * 输入：<偏移量，内容>
     * 输出：<关键词，1>
     */
    public static class UrlCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable mapOutputValue = new LongWritable(1);
        private Text urlT = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // TODO
            //根据日志文件分析，第六个字段是url
            String url=value.toString().split("\t")[5];
            urlT.set(url);
            context.write(urlT,mapOutputValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }


    //step 2: Reduce Class

    /**
     * 按次搜索词频率统计
     * 输入：<关键词，频率>
     * 输出：<频率，关键词>
     */
    public static class UrlCountReducer extends Reducer<Text, LongWritable, LongWritable, Text> {
        private final static LongWritable outputValue = new LongWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //TODO


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
        job.setMapperClass(UrlCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //3.3 reduce
        job.setNumReduceTasks(1);
        job.setReducerClass(UrlCountReducer.class);
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
