package com.opstty.mapper;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostTreesMapper extends Mapper<Object, Text, NullWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), value);
    }
}
