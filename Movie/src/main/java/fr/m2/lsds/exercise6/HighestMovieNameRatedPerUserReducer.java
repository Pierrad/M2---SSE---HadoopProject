package fr.m2.lsds.exercise6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HighestMovieNameRatedPerUserReducer extends Reducer<Text, MovieWritable, Text, Text> {
    private Map<String, String> userBestMovie = new HashMap<>();
    private Map<String, Double> userMaxRating = new HashMap<>();

    @Override
    public void reduce(Text movieId, Iterable<MovieWritable> values, Context context) throws IOException, InterruptedException {
        String movieName = null;
        Map<String, Double> userRatings = new HashMap<>();

        for (MovieWritable value : values) {
            if (value.getType().toString().equals("M")) {
                movieName = value.getValue().toString();
            } else if (value.getType().toString().equals("R")) {
                String[] parts = value.getValue().toString().split("\\|");
                String userId = parts[0];
                double rating = Double.parseDouble(parts[1]);
                userRatings.put(userId, rating);
            }
        }

        if (movieName != null) {
            for (Map.Entry<String, Double> entry : userRatings.entrySet()) {
                String userId = entry.getKey();
                double rating = entry.getValue();

                if (!userMaxRating.containsKey(userId) || rating > userMaxRating.get(userId)) {
                    userMaxRating.put(userId, rating);
                    userBestMovie.put(userId, movieName);
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, String> entry : userBestMovie.entrySet()) {
            String userId = entry.getKey();
            String movieName = entry.getValue();
            context.write(new Text(userId), new Text(movieName));
        }
    }
}
