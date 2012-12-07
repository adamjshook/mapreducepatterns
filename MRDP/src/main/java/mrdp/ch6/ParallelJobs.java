package mrdp.ch6;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ParallelJobs {

	public static class AverageReputationMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		private static final Text GROUP_ALL_KEY = new Text(
				"Average Reputation:");
		private DoubleWritable outvalue = new DoubleWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				// Split the line into tokens
				String[] tokens = value.toString().split("\t");

				// Get the reputation from the third column
				double reputation = Double.parseDouble(tokens[2]);

				// Set the output value and write to context
				outvalue.set(reputation);
				context.write(GROUP_ALL_KEY, outvalue);
			} catch (NumberFormatException e) {
				// Skip record
			}
		}
	}

	public static class AverageReputationReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private DoubleWritable outvalue = new DoubleWritable();

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			double sum = 0.0;
			double count = 0;
			for (DoubleWritable dw : values) {
				sum += dw.get();
				++count;
			}

			outvalue.set(sum / count);
			context.write(key, outvalue);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err
					.println("Usage: ParallelJobs <below-avg-in> <below-avg-out> <below-avg-out> <above-avg-out>");
			System.exit(2);
		}

		Path belowAvgInputDir = new Path(otherArgs[0]);
		Path aboveAvgInputDir = new Path(otherArgs[1]);

		Path belowAvgOutputDir = new Path(otherArgs[2]);
		Path aboveAvgOutputDir = new Path(otherArgs[3]);

		Job belowAvgJob = submitJob(conf, belowAvgInputDir, belowAvgOutputDir);
		Job aboveAvgJob = submitJob(conf, aboveAvgInputDir, aboveAvgOutputDir);

		// While both jobs are not finished, sleep
		while (!belowAvgJob.isComplete() || !aboveAvgJob.isComplete()) {
			Thread.sleep(5000);
		}

		if (belowAvgJob.isSuccessful()) {
			System.out.println("Below average job completed successfully!");
		} else {
			System.out.println("Below average job failed!");
		}

		if (aboveAvgJob.isSuccessful()) {
			System.out.println("Above average job completed successfully!");
		} else {
			System.out.println("Above average job failed!");
		}

		System.exit(belowAvgJob.isSuccessful() && aboveAvgJob.isSuccessful() ? 0
				: 1);
	}

	private static Job submitJob(Configuration conf, Path inputDir,
			Path outputDir) throws IOException, InterruptedException,
			ClassNotFoundException {

		Job job = new Job(conf, "ParallelJobs");
		job.setJarByClass(ParallelJobs.class);

		job.setMapperClass(AverageReputationMapper.class);
		job.setReducerClass(AverageReputationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, inputDir);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.submit();
		return job;
	}
}
