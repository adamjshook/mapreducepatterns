package mrdp.ch3;

import java.io.IOException;
import java.util.Map;

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

public class DistinctUserDriver {

	public static class SODistinctUserMapper extends
			Mapper<Object, Text, Text, NullWritable> {

		private Text outUserId = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input into a nice map.
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			// Get the value for the UserId attribute
			String userId = parsed.get("UserId");

			// If it is null, skip this record
			if (userId == null) {
				return;
			}

			// Otherwise, set our output key to the user's id
			outUserId.set(userId);

			// Write the user's id with a null value
			context.write(outUserId, NullWritable.get());
		}
	}

	public static class SODistinctUserReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {

			// Write the user's id with a null value
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UniqueUserCount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "StackOverflow Distinct Users");
		job.setJarByClass(DistinctUserDriver.class);
		job.setMapperClass(SODistinctUserMapper.class);
		job.setCombinerClass(SODistinctUserReducer.class);
		job.setReducerClass(SODistinctUserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
