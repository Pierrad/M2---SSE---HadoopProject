package fr.m2.lsds.exercise6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class HighestMovieNameRatedPerUserMapper extends Mapper<Object, Text, Text, MovieWritable> {
    private Text movieId = new Text();
    private MovieWritable movieData = new MovieWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        if (line.startsWith("userId") || line.startsWith("movieId")) {
            return;
        }

        String[] fields = line.split(",");
        if (fileName.contains("ratings.csv")) {
            if (fields.length >= 3) {
                movieId.set(fields[1].trim());
                String userIdRating = fields[0].trim() + "|" + fields[2].trim();
                movieData = new MovieWritable("R", userIdRating);
                context.write(movieId, movieData);
            }
        } else if (fileName.contains("movies.csv")) {
            if (fields.length >= 2) {
                movieId.set(fields[0].trim());
                String movieName = fields[1].trim();
                movieData = new MovieWritable("M", movieName);
                context.write(movieId, movieData);
            }
        }
    }
}
