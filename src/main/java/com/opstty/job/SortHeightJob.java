package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opstty.mapper.SortHeightMapper;
import com.opstty.reducer.SortHeightReducer;

public class SortHeightJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: SortHeightJob <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "sort heights job");

        job.setJarByClass(SortHeightJob.class);
        job.setMapperClass(SortHeightMapper.class);
        job.setReducerClass(SortHeightReducer.class);

        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
