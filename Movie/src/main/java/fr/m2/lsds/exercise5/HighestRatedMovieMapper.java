package fr.m2.lsds.exercise5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestRatedMovieMapper extends Mapper<Object, Text, Text, MovieRatingWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith("userId")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length >= 3) {
            String userId = fields[0].trim();
            String movieId = fields[1].trim();
            double rating = Double.parseDouble(fields[2].trim());

            context.write(new Text(userId), new MovieRatingWritable(movieId, rating));
        }
    }
}
