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
import java.util.HashMap;
import java.util.Map;

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
        JobArgs jobArgs = new JobArgs();
        try {
            jobArgs.fromArgs(pathArgs);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
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

        LrcReducer.N = jobArgs.N;

        for (String path : jobArgs.input)
        {
            FileInputFormat.addInputPath(job, new Path(path));
        }
        FileOutputFormat.setOutputPath(job, new Path(jobArgs.output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class JobArgs {
        public int N = 25;
        public String[] input;
        public String output;

        public void fromArgs(String[] args) {
            Map<String, String> entries = parseArgs(args);

            if (!entries.containsKey("input")) {
                throw new IllegalArgumentException(
                        "Missing input argument. --input=<comma separated list of input directories/files>.");
            }

            input = entries.get("input").split(",");

            if (!entries.containsKey("output")) {
                throw new IllegalArgumentException(
                        "Missing output argument. --output=<output directory>.");
            }

            output = entries.get("output");

            if (entries.containsKey("n")) {
                N = Integer.parseInt(entries.get("n"));
            }
        }

        private Map<String, String> parseArgs(String[] args) {
            Map<String, String> map = new HashMap<>();
            for (String arg: args) {
                String[] tokens = arg.split("=", 2);
                if (tokens.length != 2) {
                    System.err.println("Invalid argument syntax: '" + arg + "'. Required syntax: --name=value.");
                    System.exit(2);
                }

                map.put(tokens[0].substring(2), tokens[1]);
            }

            return map;
        }
    }
}
