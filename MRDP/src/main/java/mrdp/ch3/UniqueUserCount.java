package mrdp.ch3;

import java.io.IOException;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class UniqueUserCount {

	public static class SODistinctUserMapper extends
			Mapper<Object, Text, Text, NullWritable> {

		private Text outUserId = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());
			String userId = parsed.get("UserId");
			if (userId == null) {
				return;
			}

			outUserId.set(userId);
			context.write(outUserId, NullWritable.get());
		}
	}

	public static class SODistinctUserReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static class SOUserCounterMapper extends
			Mapper<Text, NullWritable, Text, IntWritable> {

		private static final Text DUMMY = new Text("Total:");
		private static final IntWritable ONE = new IntWritable(1);

		@Override
		public void map(Text key, NullWritable value, Context context)
				throws IOException, InterruptedException {

			context.write(DUMMY, ONE);
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

		Path tmpout = new Path(otherArgs[1] + "_tmp");
		FileSystem.get(new Configuration()).delete(tmpout, true);
		Path finalout = new Path(otherArgs[1]);
		Job job = new Job(conf, "StackOverflow Unique User Count");
		job.setJarByClass(UniqueUserCount.class);
		job.setMapperClass(SODistinctUserMapper.class);
		job.setCombinerClass(SODistinctUserReducer.class);
		job.setReducerClass(SODistinctUserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, tmpout);

		boolean exitCode = job.waitForCompletion(true);
		if (exitCode) {
			job = new Job(conf, "Stack Overflow Unique User Count");
			job.setJarByClass(UniqueUserCount.class);
			job.setMapperClass(SOUserCounterMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(job, tmpout);
			FileOutputFormat.setOutputPath(job, finalout);
			exitCode = job.waitForCompletion(true);
		}

		System.exit(exitCode ? 0 : 1);
	}
}
