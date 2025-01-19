package fr.m2.lsds.exercise1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Arrays;

public class CountMovieByGenreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) {
        try {
            String line = value.toString();

            if (key.get() == 0 && line.startsWith("movieId")) {
                return;
            }

            System.out.println("line: " + line);

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            System.out.println("fields: " + Arrays.toString(fields));

            if (fields.length >= 3) {
                String genresField = fields[2].trim();

                String[] genres = genresField.split("\\|");
                for (String genre : genres) {
                    if (!genre.isEmpty()) {
                        context.write(new Text(genre), ONE);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
