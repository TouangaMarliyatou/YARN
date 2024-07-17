package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opstty.mapper.MostTreesMapper;
import com.opstty.mapper.TreeMapper;
import com.opstty.reducer.MostTreesReducer;
import com.opstty.reducer.TreeReducer;

public class MostTreesJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // First job to count trees per district
        Job job1 = Job.getInstance(conf, "Tree Count");
        job1.setJarByClass(TreeCountJob.class);
        job1.setMapperClass(TreeMapper.class);
        job1.setCombinerClass(TreeReducer.class);
        job1.setReducerClass(TreeReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // Second job to find the district with the most trees
        Job job2 = Job.getInstance(conf, "Max Tree Count");
        job2.setJarByClass(TreeCountJob.class);
        job2.setMapperClass(MostTreesMapper.class);
        job2.setReducerClass(MostTreesReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

