package fr.m2.lsds.project.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieSortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private Text result = new Text();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder movieList = new StringBuilder();
        for (Text value : values) {
            if (movieList.length() > 0) {
                movieList.append(" ");
            }
            movieList.append(value.toString());
        }
        result.set(movieList.toString());
        context.write(key, result);
    }
}
