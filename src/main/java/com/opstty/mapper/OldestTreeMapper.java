package com.opstty.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OldestTreeMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static int YEAR_COLUMN_INDEX = 5;
    private final static int DISTRICT_COLUMN_INDEX = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("GEOPOINT")) {
            // Skip header row
            return;
        }

        String[] columns = value.toString().split(";");
        if (columns.length > YEAR_COLUMN_INDEX && columns.length > DISTRICT_COLUMN_INDEX) {
            String yearStr = columns[YEAR_COLUMN_INDEX].trim();
            String district = columns[DISTRICT_COLUMN_INDEX].trim();

            context.write(new Text("oldest_tree"), new Text(yearStr + "_" + district));
        }
    }
}