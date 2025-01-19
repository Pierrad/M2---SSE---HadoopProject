package fr.m2.lsds.exercise2;

import fr.m2.lsds.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ListMoviesByGenre extends JobBuilder {
    String genre = null;

    public ListMoviesByGenre(String genre) {
        this.genre = genre;
    }

    public Job getJob(Configuration c) throws IOException {
        c.set("filter.genre", genre);
        Job j = new Job(c, "listMoviesByGenre");
        j.setJarByClass(ListMoviesByGenre.class);
        j.setMapperClass(ListMoviesByGenreMapper.class);
        j.setReducerClass(ListMoviesByGenreReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        return j;
    }

    public void constructJob(Job job, String[] files) throws IOException {
        if (files.length < 4) {
            System.err.println("Usage: listMoviesByGenre <input path> <output path>");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(files[1]));
        FileOutputFormat.setOutputPath(job, new Path(files[2]));
    }
}
