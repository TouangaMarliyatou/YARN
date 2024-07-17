package com.opstty.reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxHeightReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable maxHeight = new FloatWritable();

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float max = Float.MIN_VALUE;
        for (FloatWritable val : values) {
            if (val.get() > max) {
                max = val.get();
            }
        }
        maxHeight.set(max);
        context.write(key, maxHeight);
    }
}
