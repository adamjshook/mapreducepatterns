package mrdp.ch6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ChainMapperDriver {

	public static final String AVERAGE_CALC_GROUP = "AverageCalculation";
	public static final String MULTIPLE_OUTPUTS_BELOW_5000 = "below5000";
	public static final String MULTIPLE_OUTPUTS_ABOVE_5000 = "above5000";

	public static class UserIdCountMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, LongWritable> {

		public static final String RECORDS_COUNTER_NAME = "Records";

		private static final LongWritable ONE = new LongWritable(1);
		private Text outkey = new Text();

		@Override
		public void map(Object key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {

			// Parse the input into a nice map.
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			// Get the value for the OwnerUserId attribute
			String userId = parsed.get("OwnerUserId");

			if (userId != null) {
				outkey.set(userId);
				output.collect(outkey, ONE);
			}
		}
	}

	public static class UserIdReputationEnrichmentMapper extends MapReduceBase
			implements Mapper<Text, LongWritable, Text, LongWritable> {

		private Text outkey = new Text();
		private HashMap<String, String> userIdToReputation = new HashMap<String, String>();

		@Override
		public void configure(JobConf job) {
			try {
				userIdToReputation.clear();
				Path[] files = DistributedCache.getLocalCacheFiles(job);

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
		public void map(Text key, LongWritable value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {

			String reputation = userIdToReputation.get(key.toString());
			if (reputation != null) {
				outkey.set(value.get() + "\t" + reputation);
				output.collect(outkey, value);
			}
		}
	}

	public static class LongSumReducer extends MapReduceBase implements
			Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable outvalue = new LongWritable();

		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			outvalue.set(sum);
			output.collect(key, outvalue);
		}
	}

	public static class UserIdBinningMapper extends MapReduceBase implements
			Mapper<Text, LongWritable, Text, LongWritable> {

		private MultipleOutputs mos = null;

		@Override
		public void configure(JobConf conf) {
			mos = new MultipleOutputs(conf);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void map(Text key, LongWritable value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {

			if (Integer.parseInt(key.toString().split("\t")[1]) < 5000) {
				mos.getCollector(MULTIPLE_OUTPUTS_BELOW_5000, reporter)
						.collect(key, value);
			} else {
				mos.getCollector(MULTIPLE_OUTPUTS_ABOVE_5000, reporter)
						.collect(key, value);
			}
		}

		@Override
		public void close() {
			try {
				mos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf("ChainMapperReducer");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err
					.println("Usage: ChainMapperReducer <posts> <users> <out>");
			System.exit(2);
		}

		Path postInput = new Path(otherArgs[0]);
		Path userInput = new Path(otherArgs[1]);
		Path outputDir = new Path(otherArgs[2]);

		// Setup first job to counter user posts
		conf.setJarByClass(ChainMapperDriver.class);

		ChainMapper.addMapper(conf, UserIdCountMapper.class,
				LongWritable.class, Text.class, Text.class, LongWritable.class,
				false, new JobConf(false));

		ChainMapper.addMapper(conf, UserIdReputationEnrichmentMapper.class,
				Text.class, LongWritable.class, Text.class, LongWritable.class,
				false, new JobConf(false));

		ChainReducer.setReducer(conf, LongSumReducer.class, Text.class,
				LongWritable.class, Text.class, LongWritable.class, false,
				new JobConf(false));

		ChainReducer.addMapper(conf, UserIdBinningMapper.class, Text.class,
				LongWritable.class, Text.class, LongWritable.class, false,
				new JobConf(false));

		conf.setCombinerClass(LongSumReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		TextInputFormat.setInputPaths(conf, postInput);

		// Configure multiple outputs
		conf.setOutputFormat(NullOutputFormat.class);
		FileOutputFormat.setOutputPath(conf, outputDir);
		MultipleOutputs.addNamedOutput(conf, MULTIPLE_OUTPUTS_ABOVE_5000,
				TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(conf, MULTIPLE_OUTPUTS_BELOW_5000,
				TextOutputFormat.class, Text.class, LongWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		// Add the user files to the DistributedCache
		FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
		for (FileStatus status : userFiles) {
			DistributedCache.addCacheFile(status.getPath().toUri(), conf);
		}

		RunningJob job = JobClient.runJob(conf);

		while (!job.isComplete()) {
			Thread.sleep(5000);
		}

		System.exit(job.isSuccessful() ? 0 : 1);
	}
}
