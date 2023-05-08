package com.app.mapreduce.alcohol;

import com.app.mapreduce.lrc.LrcJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;
import java.util.UUID;

public class Alcohol {

    public static void main(String[] args) throws Exception {
        InputStream configFileStream = LrcJob.class.getResourceAsStream("/application.properties");
        PropertyConfigurator.configure(configFileStream);

        Configuration conf = new Configuration();
        String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (pathArgs.length < 2) {
            System.err.println("Usage: alcohol-map-reduce <input-path> [...] <output-path>");
            System.exit(2);
        }
        Job wcJob = Job.getInstance(conf, "MapReduce Alcohol");
        wcJob.setJarByClass(Alcohol.class);
        wcJob.setMapperClass(AlcoholMapper.class);
        wcJob.setCombinerClass(AlcoholReducer.class);
        wcJob.setReducerClass(AlcoholReducer.class);
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(Text.class);
        for (int i = 0; i < pathArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(wcJob, new Path(pathArgs[i]));
        }
        FileOutputFormat.setOutputPath(wcJob, new Path(pathArgs[pathArgs.length - 1] + UUID.randomUUID()));
        System.exit(wcJob.waitForCompletion(true) ? 0 : 1);
    }

}