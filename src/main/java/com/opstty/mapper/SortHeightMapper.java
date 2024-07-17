package com.opstty.mapper;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortHeightMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private Text kind = new Text();
    private FloatWritable height = new FloatWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(";");
        if (parts.length > 7) {
            String kindOfTree = parts[2]; 
            try {
                float treeHeight = Float.parseFloat(parts[6]); 
                kind.set(kindOfTree);
                height.set(treeHeight);
                context.write(height, kind);
            } catch (NumberFormatException e) {
            }
        }
    }
}
