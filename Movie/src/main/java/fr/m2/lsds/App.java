package fr.m2.lsds;

import fr.m2.lsds.exercise1.CountMovieByGenre;
import fr.m2.lsds.exercise2.ListMoviesByGenre;
import fr.m2.lsds.exercise3.AverageScorePerUserID;
import fr.m2.lsds.exercise4.AverageScorePerMovieID;
import fr.m2.lsds.exercise5.HighestRatedMoviePerUser;
import fr.m2.lsds.exercise6.HighestMovieNameRatedPerUser;
import fr.m2.lsds.project.job1.MovieLikeJob;
import fr.m2.lsds.project.job2.MovieSortJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class App
{
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration c = new Configuration();
        String[] inputs = new GenericOptionsParser(c, args).getRemainingArgs();

        if (inputs.length == 0) {
            help();
            System.exit(-1);
        }

        if (inputs.length < 3) {
            System.err.println("Usage: hadoop jar <jar file> fr.m2.lsds.App <exercise number> <input path> <output path> [extra params]");
            System.exit(-1);
        }

        JobBuilder jb = null;

        String exercise = inputs[0];

        switch (exercise) {
            case "1":
                jb = new CountMovieByGenre();
                break;
            case "2":
                jb = new ListMoviesByGenre(inputs[3]);
                break;
            case "3":
                jb = new AverageScorePerUserID();
                break;
            case "4":
                jb = new AverageScorePerMovieID();
                break;
            case "5":
                jb = new HighestRatedMoviePerUser();
                break;
            case "6":
                jb = new HighestMovieNameRatedPerUser();
                break;
            case "7":
                jb = new MovieLikeJob();
                break;
            case "8":
                jb = new MovieSortJob();
                break;
            default:
                System.err.println("Invalid exercise number");
                System.exit(-1);
        }

        Job job = jb.getJob(c);
        jb.constructJob(job, inputs);

        boolean res = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void help() {
        System.out.println("Usage: hadoop jar <jar file> fr.m2.lsds.App <exercise number> <input path> <output path> [extra params]");
        System.out.println("Exercises:");
        System.out.println("1: CountMovieByGenre");
        System.out.println("2: ListMoviesByGenre");
        System.out.println("3: AverageScorePerUserID");
        System.out.println("4: AverageScorePerMovieID");
        System.out.println("5: HighestRatedMoviePerUser");
        System.out.println("6: HighestMovieNameRatedPerUser");
        System.out.println("7: Project > MovieLikeJob");
        System.out.println("8: Project > MovieSortJob");
    }
}
