package fr.m2.lsds.exercise5;

import fr.m2.lsds.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HighestRatedMoviePerUser extends JobBuilder {

    public Job getJob(Configuration c) throws IOException {
        Job job = new Job(c, "highestRatedMoviePerUser");
        job.setJarByClass(HighestRatedMoviePerUser.class);
        job.setMapperClass(HighestRatedMovieMapper.class);
        job.setReducerClass(HighestRatedMovieReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieRatingWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MovieRatingWritable.class);

        return job;
    }



    public void constructJob(Job job, String[] files) throws IOException {
        if (files.length < 3) {
            System.err.println("Usage: highestRatedMoviePerUser <input path> <output path>");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(files[1]));
        FileOutputFormat.setOutputPath(job, new Path(files[2]));
    }
}
