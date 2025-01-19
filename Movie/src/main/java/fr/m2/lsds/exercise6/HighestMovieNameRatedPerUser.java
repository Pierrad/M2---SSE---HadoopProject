package fr.m2.lsds.exercise6;

import fr.m2.lsds.JobBuilder;
import fr.m2.lsds.exercise5.MovieRatingWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HighestMovieNameRatedPerUser extends JobBuilder {
    public Job getJob(Configuration c) throws IOException {
        Job job = new Job(c, "highestMovieNameRatedPerUser");
        job.setJarByClass(HighestMovieNameRatedPerUser.class);

        job.setMapperClass(HighestMovieNameRatedPerUserMapper.class);
        job.setReducerClass(HighestMovieNameRatedPerUserReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public void constructJob(Job job, String[] files) throws IOException {
        if (files.length < 4) {
            System.err.println("Usage: highestMovieNameRatedPerUser <input path> <output path>");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(files[1]));
        FileInputFormat.addInputPath(job, new Path(files[2]));
        FileOutputFormat.setOutputPath(job, new Path(files[3]));
    }
}
