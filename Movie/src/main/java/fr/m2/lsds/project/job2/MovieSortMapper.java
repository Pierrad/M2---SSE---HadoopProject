package fr.m2.lsds.project.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieSortMapper extends Mapper<Object, Text, IntWritable, Text> {

    private IntWritable count = new IntWritable();
    private Text movieName = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        int lastTabIndex = line.lastIndexOf('\t');
        if (lastTabIndex != -1) {
            String movie = line.substring(0, lastTabIndex).trim();
            String countStr = line.substring(lastTabIndex + 1).trim();

            try {
                count.set(Integer.parseInt(countStr));
                movieName.set(movie);
                context.write(count, movieName);
            } catch (NumberFormatException e) {
                System.err.println("Error parsing count: " + countStr);
            }
        }
    }
}
