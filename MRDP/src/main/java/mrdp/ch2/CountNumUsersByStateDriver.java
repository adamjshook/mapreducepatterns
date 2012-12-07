package mrdp.ch2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CountNumUsersByStateDriver {

	public static class CountNumUsersByStateMapper extends
			Mapper<Object, Text, NullWritable, NullWritable> {

		public static final String STATE_COUNTER_GROUP = "State";

		private String[] statesArray = new String[] { "AL", "AK", "AZ", "AR",
				"CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
				"IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
				"MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
				"OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
				"VT", "VA", "WA", "WV", "WI", "WY" };

		private HashSet<String> states = new HashSet<String>(
				Arrays.asList(statesArray));

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input into a nice map.
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			// Get the value for the Location attribute
			String location = parsed.get("Location");

			// Look for a state abbreviation code if the location is not null or
			// empty
			if (location != null && !location.isEmpty()) {
				boolean unknown = true;
				// Make location uppercase and split on white space
				String[] tokens = location.toUpperCase().split("\\s");
				// For each token
				for (String state : tokens) {
					// Check if it is a state
					if (states.contains(state)) {

						// If so, increment the state's counter by 1 and flag it
						// as not unknown
						context.getCounter(STATE_COUNTER_GROUP, state)
								.increment(1);
						unknown = false;
						break;
					}
				}

				// If the state is unknown, increment the counter
				if (unknown) {
					context.getCounter(STATE_COUNTER_GROUP, "Unknown")
							.increment(1);
				}
			} else {
				// If it is empty or null, increment the counter by 1
				context.getCounter(STATE_COUNTER_GROUP, "NullOrEmpty")
						.increment(1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: CountNumUsersByState <users> <out>");
			System.exit(2);
		}

		Path input = new Path(otherArgs[0]);
		Path outputDir = new Path(otherArgs[1]);

		Job job = new Job(conf, "Count Num Users By State");
		job.setJarByClass(CountNumUsersByStateDriver.class);

		job.setMapperClass(CountNumUsersByStateMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		int code = job.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			for (Counter counter : job.getCounters().getGroup(
					CountNumUsersByStateMapper.STATE_COUNTER_GROUP)) {
				System.out.println(counter.getDisplayName() + "\t"
						+ counter.getValue());
			}
		}

		// Clean up empty output directory
		FileSystem.get(conf).delete(outputDir, true);

		System.exit(code);
	}
}
