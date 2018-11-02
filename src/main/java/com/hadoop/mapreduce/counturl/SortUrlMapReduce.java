package com.hadoop.mapreduce.counturl;

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

import javax.sound.midi.SoundbankResource;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

public class SortUrlMapReduce extends Configured implements Tool {
    //step 1: Map Class

    /**
     * 输入：<偏移量，词频+网址>
     * 输出：<词频，网址>
     */
    public static class SortUrlMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable mapOutputKey = new LongWritable();
        private Text mapOutputValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 提取出urlcount的结果
            String url[] = value.toString().split("\t");
            long fre = Long.parseLong(url[0]);
            mapOutputKey.set(fre);
            mapOutputValue.set(url[1]);
            context.write(mapOutputKey, mapOutputValue);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }


    /**
     * 输入：<词频，网址>
     * 输出：<词频，网址>
     */
    //step 2: Reduce Class
    public static class SortUrlReducer extends Reducer<LongWritable, Text, Text, Text> {
        public static Long getTOTAL() {
            return TOTAL;
        }

        public static void setTOTAL(Long TOTAL) {
            SortUrlReducer.TOTAL = TOTAL;
        }

        private static Long TOTAL = 0L;
        private Text url = new Text();
        private Text dw = new Text();
        private int time = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // 从配置中读取totoal
            setTOTAL(Long.parseLong(context.getConfiguration().get("total")));
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //TODO
            Iterator<Text> var4 = values.iterator();
            DecimalFormat decimalFormat = new DecimalFormat("0.000%");
            while (var4.hasNext()) {
                Text value = var4.next();
                url.set(value);
                double v = (double) key.get() / (double) getTOTAL();
                System.out.println("======>"+v);
                dw.set(decimalFormat.format(v));
                context.write(url, dw);
            }

        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            this.setup(context);
            try {
                while (context.nextKey()) {
                    if (time < 10) {
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

    private Long total = 0l;

    public SortUrlMapReduce(Long total) {
        this.total = total;
    }

    //step 3: Driver ,component job
    public int run(String[] args) throws Exception {

        //1.get configuration
        Configuration configuration = getConf();
        configuration.set("total", total.toString());
        System.out.println("---------------->" + total);
        //2.create Job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        //3.set Job
        //3.1 input
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        //3.2 map
        job.setMapperClass(SortUrlMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setSortComparatorClass(SortUrlComparator.class);
        //3.3 reduce
        job.setReducerClass(SortUrlReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //3.4 output
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);
        //4.submit job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }
}
