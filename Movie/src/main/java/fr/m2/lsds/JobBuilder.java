package fr.m2.lsds;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public abstract class JobBuilder {
    public abstract Job getJob(Configuration c) throws IOException;
    public abstract void constructJob(Job job, String[] files) throws IOException;
}
