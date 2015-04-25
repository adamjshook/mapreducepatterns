package com.oreilly.mrdp.ch6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.oreilly.mrdp.utils.MRDPUtils;

public class ChainMapperDriver {

    public static final String AVERAGE_CALC_GROUP = "AverageCalculation";
    public static final String MULTIPLE_OUTPUTS_BELOW_5000 = "below5000";
    public static final String MULTIPLE_OUTPUTS_ABOVE_5000 = "above5000";

    public static class UserIdCountMapper extends
            Mapper<Object, Text, Text, LongWritable> {

        public static final String RECORDS_COUNTER_NAME = "Records";

        private static final LongWritable ONE = new LongWritable(1);
        private Text outkey = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws InterruptedException, IOException {

            // Parse the input into a nice map.
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                    .toString());

            // Get the value for the OwnerUserId attribute
            String userId = parsed.get("OwnerUserId");

            if (userId != null) {
                outkey.set(userId);
                context.write(outkey, ONE);
            }
        }
    }

    public static class UserIdReputationEnrichmentMapper extends
            Mapper<Text, LongWritable, Text, LongWritable> {

        private Text outkey = new Text();
        private HashMap<String, String> userIdToReputation = new HashMap<String, String>();

        @Override
        protected void setup(Context context) {
            try {
                userIdToReputation.clear();
                URI[] files = context.getCacheFiles();

                if (files == null || files.length == 0) {
                    throw new RuntimeException(
                            "User information is not set in DistributedCache");
                }

                // Read all files in the DistributedCache
                for (URI p : files) {
                    BufferedReader rdr = new BufferedReader(
                            new InputStreamReader(
                                    new GZIPInputStream(new FileInputStream(
                                            new File(p.toString())))));

                    String line;
                    // For each record in the user file
                    while ((line = rdr.readLine()) != null) {

                        // Get the user ID and reputation
                        Map<String, String> parsed = MRDPUtils
                                .transformXmlToMap(line);
                        String userId = parsed.get("Id");
                        String reputation = parsed.get("Reputation");

                        if (userId != null && reputation != null) {
                            // Map the user ID to the reputation
                            userIdToReputation.put(userId, reputation);
                        }
                    }

                    rdr.close();
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void map(Text key, LongWritable value, Context context)
                throws InterruptedException, IOException {

            String reputation = userIdToReputation.get(key.toString());
            if (reputation != null) {
                outkey.set(value.get() + "\t" + reputation);
                context.write(outkey, value);
            }
        }
    }

    public static class LongSumReducer extends
            Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable outvalue = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                Context context) throws InterruptedException, IOException {

            int sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            outvalue.set(sum);
            context.write(key, outvalue);
        }
    }

    public static class UserIdBinningMapper extends
            Mapper<Text, LongWritable, Text, LongWritable> {

        private MultipleOutputs<Text, LongWritable> mos = null;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<Text, LongWritable>(context);
        }

        @Override
        protected void map(Text key, LongWritable value, Context context)
                throws InterruptedException, IOException {

            if (Integer.parseInt(key.toString().split("\t")[1]) < 5000) {
                mos.write(MULTIPLE_OUTPUTS_BELOW_5000, key, value);
            } else {
                mos.write(MULTIPLE_OUTPUTS_ABOVE_5000, key, value);
            }
        }

        @Override
        protected void cleanup(Context context) throws InterruptedException,
                IOException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err
                    .println("Usage: ChainMapperReducer <posts> <users> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "ChainMapperReducer");

        Path postInput = new Path(otherArgs[0]);
        Path userInput = new Path(otherArgs[1]);
        Path outputDir = new Path(otherArgs[2]);

        // Setup first job to counter user posts
        job.setJarByClass(ChainMapperDriver.class);

        ChainMapper
                .addMapper(job, UserIdCountMapper.class, LongWritable.class,
                        Text.class, Text.class, LongWritable.class,
                        new Configuration());

        ChainMapper.addMapper(job, UserIdReputationEnrichmentMapper.class,
                Text.class, LongWritable.class, Text.class, LongWritable.class,
                new Configuration());

        ChainReducer.setReducer(job, LongSumReducer.class, Text.class,
                LongWritable.class, Text.class, LongWritable.class,
                new Configuration());

        ChainReducer.addMapper(job, UserIdBinningMapper.class, Text.class,
                LongWritable.class, Text.class, LongWritable.class,
                new Configuration());

        job.setCombinerClass(LongSumReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, postInput);

        // Configure multiple outputs
        job.setOutputFormatClass(NullOutputFormat.class);

        FileOutputFormat.setOutputPath(job, outputDir);

        MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_ABOVE_5000,
                TextOutputFormat.class, Text.class, LongWritable.class);

        MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_BELOW_5000,
                TextOutputFormat.class, Text.class, LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Add the user files to the DistributedCache
        FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
        for (FileStatus status : userFiles) {
            job.addCacheFile(status.getPath().toUri());
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
