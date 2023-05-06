package com.app.mapreduce.lrc;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;

@Slf4j
public class LrcJob {
    public static void main(String[] args) throws Exception {
        InputStream configFileStream = LrcJob.class.getResourceAsStream("/application.properties");
        if (configFileStream != null) {
            PropertyConfigurator.configure(configFileStream);
            configFileStream.close();
        }

        Configuration conf = new Configuration();
        String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (pathArgs.length < 2)
        {
            System.err.println("Usage: lrc-map-reduce <input-path> [...] <output-path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapReduce LRC");
        job.setJarByClass(LrcJob.class);
        job.setMapperClass(LrcMapper.class);
        //job.setCombinerClass(LrcReducer.class);
        job.setReducerClass(LrcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LrcMapperValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        LrcReducer.N = 5;

        for (int i = 0; i < pathArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job, new Path(pathArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(pathArgs[pathArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
