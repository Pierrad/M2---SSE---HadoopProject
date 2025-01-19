package fr.m2.lsds.project.job1;

import fr.m2.lsds.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MovieLikeJob extends JobBuilder {
    public Job getJob(Configuration c) throws IOException {
        Job job = new Job(c, "movieLikeJob");
        job.setJarByClass(MovieLikeJob.class);

        job.setMapperClass(MovieLikeMapper.class);
        job.setReducerClass(MovieLikeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    public void constructJob(Job job, String[] files) throws IOException {
        if (files.length < 3) {
            System.err.println("Usage: movieLikeJob <input path> <output path>");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(files[1]));
        FileOutputFormat.setOutputPath(job, new Path(files[2]));
    }
}
