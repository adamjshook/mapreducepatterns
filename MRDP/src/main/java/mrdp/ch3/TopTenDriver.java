package mrdp.ch3;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenDriver {

	public static class SOTopTenMapper extends
			Mapper<Object, Text, NullWritable, Text> {
		// Our output key and value Writables
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());
			if (parsed == null) {
				return;
			}

			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");

			// Get will return null if the key is not there
			if (userId == null || reputation == null) {
				// skip this record
				return;
			}

			repToRecordMap.put(Integer.parseInt(reputation), new Text(value));

			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static class SOTopTenReducer extends
			Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
						.toString());

				repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")),
						new Text(value));

				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

			for (Text t : repToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopTenDriver <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Top Ten Users by Reputation");
		job.setJarByClass(TopTenDriver.class);
		job.setMapperClass(SOTopTenMapper.class);
		job.setReducerClass(SOTopTenReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
