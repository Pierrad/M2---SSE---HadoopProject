package fr.m2.lsds.exercise3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageScorePerUserIDReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text userId, Iterable<DoubleWritable> ratings, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        for (DoubleWritable rating : ratings) {
            sum += rating.get();
            count++;
        }

        double average = count == 0 ? 0 : sum / count;

        context.write(userId, new DoubleWritable(average));
    }
}