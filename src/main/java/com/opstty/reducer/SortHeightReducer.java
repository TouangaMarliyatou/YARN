package com.opstty.reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortHeightReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {
    @Override
    protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(key, val);
        }
    }
}
