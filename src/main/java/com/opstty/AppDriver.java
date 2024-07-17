package com.opstty;

import org.apache.hadoop.util.ProgramDriver;

import com.opstty.job.DistrictsJob;
import com.opstty.job.MaxHeightJob;
import com.opstty.job.MostTreesJob;
import com.opstty.job.OldestTreeJob;
import com.opstty.job.SortHeightJob;
import com.opstty.job.SpeciesJob;
import com.opstty.job.TreeCountJob;
import com.opstty.job.WordCount;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("DistrictsJob", DistrictsJob.class,
                    "A map/reduce program that displays the list of distinct containing trees in the input file.");
            programDriver.addClass("SpeciesJob", SpeciesJob.class,
                    "A mapReduce program that displays the list of different species trees in the input file.");
            programDriver.addClass("TreeCountJob", TreeCountJob.class,
                    "A mapReduce program that calculates the number of trees of each kinds in the input file.");
            programDriver.addClass("MaxHeightJob", MaxHeightJob.class,
                    "A mapReduce program that calculates the height of the tallest tree of each kind in the input file.");
            programDriver.addClass("SortHeightJob", SortHeightJob.class,
                    "A mapReduce program that displays the district that contains the most trees in the input file.");
            programDriver.addClass("OldestTreeJob", OldestTreeJob.class,
                    "A mapReduce program that t displays the district where the oldest tree is in the input file.");
            programDriver.addClass("MostTreesJob", MostTreesJob.class,
                    "A mapReduce program that t displays the district where the oldest tree is in the input file.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
