package fr.m2.lsds.exercise4;

import fr.m2.lsds.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AverageScorePerMovieID extends JobBuilder {

    public Job getJob(Configuration c) throws IOException {
        Job j = new Job(c, "averageScorePerMovieID");
        j.setJarByClass(AverageScorePerMovieID.class);
        j.setMapperClass(AverageScorePerMovieIDMapper.class);
        j.setReducerClass(AverageScorePerMovieIDReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        return j;
    }

    public void constructJob(Job job, String[] files) throws IOException {
        if (files.length < 3) {
            System.err.println("Usage: averageScorePerMovieID <input path> <output path>");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(files[1]));
        FileOutputFormat.setOutputPath(job, new Path(files[2]));
    }
}
