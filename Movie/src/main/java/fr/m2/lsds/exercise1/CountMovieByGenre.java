package fr.m2.lsds.exercise1;

import fr.m2.lsds.App;
import fr.m2.lsds.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CountMovieByGenre extends JobBuilder {
    public Job getJob(Configuration c) throws IOException {
        Job j = new Job(c, "countMovieByGenre");
        j.setJarByClass(App.class);
        j.setMapperClass(CountMovieByGenreMapper.class);
        j.setReducerClass(CountMovieByGenreReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        return j;
    }

    public void constructJob(Job job, String[] files) throws IOException {
        if (files.length < 3) {
            System.err.println("Usage: countMovieByGenre <input path> <output path>");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(files[1]));
        FileOutputFormat.setOutputPath(job, new Path(files[2]));
    }
}
