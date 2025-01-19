package fr.m2.lsds.exercise3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageScorePerUserIDMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            if (key.get() == 0 && line.startsWith("userId")) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length >= 4) {
                String userId = fields[0].trim();
                double rating = Double.parseDouble(fields[2].trim());

                context.write(new Text(userId), new DoubleWritable(rating));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
