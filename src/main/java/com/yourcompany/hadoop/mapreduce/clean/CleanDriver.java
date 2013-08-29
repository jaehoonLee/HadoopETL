package com.yourcompany.hadoop.mapreduce.clean;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: leejaehoon
 * Date: 13. 8. 20.
 * Time: 오후 11:02
 */
public class CleanDriver extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CleanDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job();
        parseArguments(args, job);

        job.setJarByClass(CleanDriver.class);

        job.setMapperClass(CleanMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void parseArguments(String[] args, Job job) throws IOException {
        for (int i = 0; i < args.length; ++i) {
            if ("-input".equals(args[i])) {
                FileInputFormat.setInputPaths(job, new Path[++i]);
            } else if ("-output".equals(args[i])) {
                FileOutputFormat.setOutputPath(job, new Path(args[++i]));
            } else if ("-hdfs".equals(args[i])) {
                job.getConfiguration().set("fs.default.name", args[++i]);
            } else if("-jobTracker".equals(args[i]))
            {
                job.getConfiguration().set("mapred.job.name", args[++i]);
            } else if("-columnToClean".equals(args[i]))
            {
                job.getConfiguration().set("columnToClean", args[++i]);
            } else if("-delimiter".equals(args[i]))
            {
                job.getConfiguration().set("delimiter", args[++i]);
            }
        }
    }


}
