package fr.m2.lsds.exercise2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ListMoviesByGenreMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String filterGenre;

    @Override
    protected void setup(Context context) {
        filterGenre = context.getConfiguration().get("filter.genre");
    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        try {
            String line = value.toString();
            if (key.get() == 0 && line.startsWith("movieId")) {
                return;
            }

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            if (fields.length >= 3) {
                String movieTitle = fields[1].trim();
                String genresField = fields[2].trim();
                String[] genres = genresField.split("\\|");

                for (String genre : genres) {
                    if (genre.equalsIgnoreCase(filterGenre)) {
                        context.write(new Text(genre), new Text(movieTitle));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
