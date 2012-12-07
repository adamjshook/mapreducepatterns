package mrdp.ch3;

import java.io.*;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SimpleRandomSampling {

	public static class SRSMapper extends
			Mapper<Object, Text, NullWritable, Text> {

		private Random rands = new Random();
		private Double percentage;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// retrieve the percentage that is passed in via the configuration
			// like this: conf.set("filter_percentage", .5); for .5%
			String strPercentage = context.getConfiguration().get(
					"filter_percentage");

			percentage = Double.parseDouble(strPercentage) / 100.0;
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			if (rands.nextDouble() < percentage) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: SRS <percentage> <in> <out>");
			System.exit(2);
		}
		conf.set("filter_percentage", otherArgs[0]);

		Job job = new Job(conf, "SRS");
		job.setJarByClass(SimpleRandomSampling.class);
		job.setMapperClass(SRSMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0); // Set number of reducers to zero
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
