package fr.m2.lsds.exercise5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestRatedMovieReducer extends Reducer<Text, MovieRatingWritable, Text, MovieRatingWritable> {

    @Override
    public void reduce(Text userId, Iterable<MovieRatingWritable> values, Context context) throws IOException, InterruptedException {
        String highestRatedMovie = "";
        double highestRating = Double.MIN_VALUE;

        for (MovieRatingWritable value : values) {
            if (value.getRating() > highestRating) {
                highestRating = value.getRating();
                highestRatedMovie = value.getMovieId();
            }
        }

        context.write(userId, new MovieRatingWritable(highestRatedMovie, highestRating));
    }
}
