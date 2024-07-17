package com.opstty.reducer;

import java.io.IOException;
import java.time.Year;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OldestTreeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String oldestDistrict = null;
        int oldestAge = Integer.MIN_VALUE;
        
        int currentYear = Year.now().getValue();

        for (Text value : values) {
            String[] parts = value.toString().split("_");
            if (parts.length < 2) {
                continue;
            }

            String yearStr = parts[0];
            String district = parts[1];

            try {
                int yearPlanted = Integer.parseInt(yearStr);

                int age = currentYear - yearPlanted;

                if (age > oldestAge) {
                    oldestAge = age;
                    oldestDistrict = district;
                }
            } catch (NumberFormatException e) {
                // Log or skip invalid year values
                System.err.println("Invalid year format: " + yearStr);
            }
        }

        if (oldestDistrict != null) {
            context.write(new Text("District with oldest tree"), new Text(oldestDistrict + " - Age: " + oldestAge));
        }
    }
}