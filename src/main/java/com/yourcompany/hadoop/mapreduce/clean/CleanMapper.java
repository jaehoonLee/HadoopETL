package com.yourcompany.hadoop.mapreduce.clean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: leejaehoon
 * Date: 13. 8. 20.
 * Time: 오후 11:03
 */
public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

    private String delimiter;
    private int columnToClean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        delimiter = configuration.get("delimiter");
        columnToClean = configuration.getInt("columnToClean", 0);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(delimiter);
        if(!validate(context, columns))
        {
            return;
        }


        StringBuilder builder = new StringBuilder();
        for (int index = 0 ; index < columns.length ; index++)
        {
            String column = columns[index];
            if(index == columnToClean) {
                continue;
            }
            builder.append(column).append(delimiter);
        }

        int outputLength = builder.toString().length();
        context.write(NullWritable.get(), new Text(builder.toString().substring(0, outputLength - 1)));

    }

    private boolean validate(Context context, String[] columns)
    {
        if(columnToClean > columns.length - 1)
        {
            context.getCounter("Validation", "INVALID").increment(1);
            return false;
        }

        context.getCounter("Validation", "VALID").increment(1);
        return true;
    }
}
