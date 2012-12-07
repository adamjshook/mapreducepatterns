package mrdp.ch4;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PartitionedUsers {

	public static class LastAccessDateMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		// This object will format the creation date string into a Date object
		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		private IntWritable outkey = new IntWritable();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			// Grab the last access date
			String strDate = parsed.get("LastAccessDate");

			// skip this record if date is null
			if (strDate != null) {
				try {
					// Parse the string into a Calendar object
					Calendar cal = Calendar.getInstance();
					cal.setTime(frmt.parse(strDate));
					outkey.set(cal.get(Calendar.YEAR));
					// Write out the year with the input value
					context.write(outkey, value);
				} catch (ParseException e) {
					// An error occurred parsing the creation Date string
					// skip this record
				}
			}
		}
	}

	public static class LastAccessDatePartitioner extends
			Partitioner<IntWritable, Text> implements Configurable {

		private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";

		private Configuration conf = null;
		private int minLastAccessDateYear = 0;

		@Override
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			return key.get() - minLastAccessDateYear;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public void setConf(Configuration conf) {
			this.conf = conf;
			minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
		}

		/**
		 * Sets the minimum possible last access date to subtract from each key
		 * to be partitioned<br>
		 * <br>
		 * 
		 * That is, if the last min access date is "2008" and the key to
		 * partition is "2009", it will go to partition 2009 - 2008 = 1
		 * 
		 * @param job
		 *            The job to configure
		 * @param minLastAccessDateYear
		 *            The minimum access date.
		 */
		public static void setMinLastAccessDate(Job job,
				int minLastAccessDateYear) {
			job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR,
					minLastAccessDateYear);
		}
	}

	public static class ValueReducer extends
			Reducer<IntWritable, Text, Text, NullWritable> {

		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: PartitionedUsers <users> <outdir>");
			System.exit(2);
		}

		Job job = new Job(conf, "PartitionedUsers");

		job.setJarByClass(PartitionedUsers.class);

		job.setMapperClass(LastAccessDateMapper.class);

		// Set custom partitioner and min last access date
		job.setPartitionerClass(LastAccessDatePartitioner.class);
		LastAccessDatePartitioner.setMinLastAccessDate(job, 2008);

		// Last access dates span between 2008-2011, or 4 years
		job.setNumReduceTasks(4);
		job.setReducerClass(ValueReducer.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapred.textoutputformat.separator", "");

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
