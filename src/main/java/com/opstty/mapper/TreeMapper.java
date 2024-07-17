package com.opstty.mapper;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TreeMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private IntWritable district = new IntWritable();
    private final static IntWritable one = new IntWritable(1);
    private boolean isHeader = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false; // Skip the header line
            return;
        }
        String line = value.toString();
        String[] parts = line.split(";"); // Assuming CSV format with semicolon delimiter
        if (parts.length > 1) {
            try {
                int districtNumber = Integer.parseInt(parts[1]); // Assuming district is the second field
                district.set(districtNumber);
                context.write(district, one);
            } catch (NumberFormatException e) {
                // Log and skip malformed records
                System.err.println("NumberFormatException: " + e.getMessage() + " in line: " + line);
            }
        }
    }
}
