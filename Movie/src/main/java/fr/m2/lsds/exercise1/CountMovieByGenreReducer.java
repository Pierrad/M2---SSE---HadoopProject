package fr.m2.lsds.exercise1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountMovieByGenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text genre, Iterable<IntWritable> values, Context con) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        try {
            con.write(genre, new IntWritable(sum));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}