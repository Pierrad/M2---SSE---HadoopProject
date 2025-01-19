package fr.m2.lsds.exercise5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieRatingWritable implements Writable {
    private String movieId;
    private double rating;

    public MovieRatingWritable() {
    }

    public MovieRatingWritable(String movieId, double rating) {
        this.movieId = movieId;
        this.rating = rating;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(movieId);
        out.writeDouble(rating);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.movieId = in.readUTF();
        this.rating = in.readDouble();
    }

    @Override
    public String toString() {
        return movieId + "\t" + rating;
    }
}
