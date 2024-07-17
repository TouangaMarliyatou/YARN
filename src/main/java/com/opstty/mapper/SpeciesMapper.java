package com.opstty.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SpeciesMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(";");
        if (parts.length > 3) { 
            String species = parts[3];
            context.write(new Text(species), new Text(""));
        }
    }
}
