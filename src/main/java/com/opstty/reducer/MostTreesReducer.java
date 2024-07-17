package com.opstty.reducer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostTreesReducer extends Reducer<NullWritable, Text, IntWritable, IntWritable> {
    private IntWritable maxDistrict = new IntWritable();
    private IntWritable maxTrees = new IntWritable();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int maxDistrictNumber = -1;
        int maxTreeCount = -1;

        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            int districtNumber = Integer.parseInt(parts[0]);
            int treeCount = Integer.parseInt(parts[1]);

            if (treeCount > maxTreeCount) {
                maxTreeCount = treeCount;
                maxDistrictNumber = districtNumber;
            }
        }

        maxDistrict.set(maxDistrictNumber);
        maxTrees.set(maxTreeCount);
        context.write(maxDistrict, maxTrees);
    }
}
