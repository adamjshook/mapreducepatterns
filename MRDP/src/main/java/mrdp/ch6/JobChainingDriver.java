package mrdp.ch6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class JobChainingDriver {

	public static final String AVERAGE_CALC_GROUP = "AverageCalculation";
	public static final String MULTIPLE_OUTPUTS_ABOVE_NAME = "aboveavg";
	public static final String MULTIPLE_OUTPUTS_BELOW_NAME = "belowavg";

	public static class UserIdCountMapper extends
			Mapper<Object, Text, Text, LongWritable> {

		public static final String RECORDS_COUNTER_NAME = "Records";

		private static final LongWritable ONE = new LongWritable(1);
		private Text outkey = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input into a nice map.
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			// Get the value for the OwnerUserId attribute
			String userId = parsed.get("OwnerUserId");

			if (userId != null) {
				outkey.set(userId);
				context.write(outkey, ONE);
				context.getCounter(AVERAGE_CALC_GROUP, RECORDS_COUNTER_NAME)
						.increment(1);
			}
		}
	}

	public static class UserIdSumReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		public static final String USERS_COUNTER_NAME = "Users";
		private LongWritable outvalue = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			// Increment user counter, as each reduce group represents one user
			context.getCounter(AVERAGE_CALC_GROUP, USERS_COUNTER_NAME)
					.increment(1);

			int sum = 0;

			for (LongWritable value : values) {
				sum += value.get();
			}

			outvalue.set(sum);
			context.write(key, outvalue);
		}
	}

	public static class UserIdBinningMapper extends
			Mapper<Object, Text, Text, Text> {

		public static final String AVERAGE_POSTS_PER_USER = "avg.posts.per.user";

		public static void setAveragePostsPerUser(Job job, double avg) {
			job.getConfiguration().set(AVERAGE_POSTS_PER_USER,
					Double.toString(avg));
		}

		public static double getAveragePostsPerUser(Configuration conf) {
			return Double.parseDouble(conf.get(AVERAGE_POSTS_PER_USER));
		}

		private double average = 0.0;
		private MultipleOutputs<Text, Text> mos = null;
		private Text outkey = new Text(), outvalue = new Text();
		private HashMap<String, String> userIdToReputation = new HashMap<String, String>();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			average = getAveragePostsPerUser(context.getConfiguration());
			mos = new MultipleOutputs<Text, Text>(context);

			try {
				Path[] files = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());

				if (files == null || files.length == 0) {
					throw new RuntimeException(
							"User information is not set in DistributedCache");
				}

				// Read all files in the DistributedCache
				for (Path p : files) {
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
				}

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split("\t");

			String userId = tokens[0];
			int posts = Integer.parseInt(tokens[1]);

			outkey.set(userId);
			outvalue.set((long) posts + "\t" + userIdToReputation.get(userId));

			if ((double) posts < average) {
				mos.write(MULTIPLE_OUTPUTS_BELOW_NAME, outkey, outvalue,
						MULTIPLE_OUTPUTS_BELOW_NAME + "/part");
			} else {
				mos.write(MULTIPLE_OUTPUTS_ABOVE_NAME, outkey, outvalue,
						MULTIPLE_OUTPUTS_ABOVE_NAME + "/part");
			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err
					.println("Usage: JobChainingDriver <posts> <users> <out>");
			System.exit(2);
		}

		Path postInput = new Path(otherArgs[0]);
		Path userInput = new Path(otherArgs[1]);
		Path outputDirIntermediate = new Path(otherArgs[2] + "_int");
		Path outputDir = new Path(otherArgs[2]);

		// Setup first job to counter user posts
		Job countingJob = new Job(conf, "JobChaining-Counting");
		countingJob.setJarByClass(JobChainingDriver.class);

		// Set our mapper and reducer, we can use the API's long sum reducer for
		// a combiner!
		countingJob.setMapperClass(UserIdCountMapper.class);
		countingJob.setCombinerClass(LongSumReducer.class);
		countingJob.setReducerClass(UserIdSumReducer.class);

		countingJob.setOutputKeyClass(Text.class);
		countingJob.setOutputValueClass(LongWritable.class);

		countingJob.setInputFormatClass(TextInputFormat.class);

		TextInputFormat.addInputPath(countingJob, postInput);

		countingJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(countingJob, outputDirIntermediate);

		// Execute job and grab exit code
		int code = countingJob.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			// Calculate the average posts per user by getting counter values
			double numRecords = (double) countingJob
					.getCounters()
					.findCounter(AVERAGE_CALC_GROUP,
							UserIdCountMapper.RECORDS_COUNTER_NAME).getValue();
			double numUsers = (double) countingJob
					.getCounters()
					.findCounter(AVERAGE_CALC_GROUP,
							UserIdSumReducer.USERS_COUNTER_NAME).getValue();

			double averagePostsPerUser = numRecords / numUsers;

			// Setup binning job
			Job binningJob = new Job(new Configuration(), "JobChaining-Binning");
			binningJob.setJarByClass(JobChainingDriver.class);

			// Set mapper and the average posts per user
			binningJob.setMapperClass(UserIdBinningMapper.class);
			UserIdBinningMapper.setAveragePostsPerUser(binningJob,
					averagePostsPerUser);

			binningJob.setNumReduceTasks(0);

			binningJob.setInputFormatClass(TextInputFormat.class);
			TextInputFormat.addInputPath(binningJob, outputDirIntermediate);

			// Add two named outputs for below/above average
			MultipleOutputs.addNamedOutput(binningJob,
					MULTIPLE_OUTPUTS_BELOW_NAME, TextOutputFormat.class,
					Text.class, Text.class);

			MultipleOutputs.addNamedOutput(binningJob,
					MULTIPLE_OUTPUTS_ABOVE_NAME, TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.setCountersEnabled(binningJob, true);

			TextOutputFormat.setOutputPath(binningJob, outputDir);

			// Add the user files to the DistributedCache
			FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
			for (FileStatus status : userFiles) {
				DistributedCache.addCacheFile(status.getPath().toUri(),
						binningJob.getConfiguration());
			}

			// Execute job and grab exit code
			code = binningJob.waitForCompletion(true) ? 0 : 1;
		}

		// Clean up the intermediate output
		FileSystem.get(conf).delete(outputDirIntermediate, true);

		System.exit(code);
	}
}
