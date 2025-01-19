package fr.m2.lsds.project.job2;

import fr.m2.lsds.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MovieSortJob extends JobBuilder {
    public Job getJob(Configuration c) throws IOException {
        Job job = new Job(c, "movieSortJob");
        job.setJarByClass(MovieSortJob.class);

        job.setMapperClass(MovieSortMapper.class);
        job.setReducerClass(MovieSortReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public void constructJob(Job job, String[] files) throws IOException {
        if (files.length < 3) {
            System.err.println("Usage: movieSortJob <input path> <output path>");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(files[1]));
        FileOutputFormat.setOutputPath(job, new Path(files[2]));
    }
}
