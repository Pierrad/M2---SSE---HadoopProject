package fr.m2.lsds.exercise2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ListMoviesByGenreReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text genre, Iterable<Text> movieTitles, Context context) throws IOException, InterruptedException {
        StringBuilder movieList = new StringBuilder();

        for (Text movieTitle : movieTitles) {
            if (movieList.length() > 0) {
                movieList.append(", ");
            }
            movieList.append(movieTitle.toString());
        }

        context.write(genre, new Text(movieList.toString()));
    }
}
