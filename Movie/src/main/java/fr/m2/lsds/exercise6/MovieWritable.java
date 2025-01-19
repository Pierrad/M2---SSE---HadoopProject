package fr.m2.lsds.exercise6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieWritable implements Writable {
    private Text type;
    private Text value;

    public MovieWritable() {
        this.type = new Text();
        this.value = new Text();
    }

    public MovieWritable(String type, String value) {
        this.type = new Text(type);
        this.value = new Text(value);
    }

    public Text getType() {
        return type;
    }

    public Text getValue() {
        return value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        type.write(out);
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type.readFields(in);
        value.readFields(in);
    }

    @Override
    public String toString() {
        return type.toString() + "|" + value.toString();
    }
}
