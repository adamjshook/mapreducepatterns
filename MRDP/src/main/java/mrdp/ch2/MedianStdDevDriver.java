package mrdp.ch2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MedianStdDevDriver {

	public static class SOMedianStdDevMapper extends
			Mapper<Object, Text, IntWritable, IntWritable> {

		private IntWritable outHour = new IntWritable();
		private IntWritable outCommentLength = new IntWritable();

		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		@SuppressWarnings("deprecation")
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strDate = parsed.get("CreationDate");

			// Grab the comment to find the length
			String text = parsed.get("Text");

			// .get will return null if the key is not there
			if (strDate == null || text == null) {
				// skip this record
				return;
			}

			try {
				// get the hour this comment was posted in
				Date creationDate = frmt.parse(strDate);
				outHour.set(creationDate.getHours());

				// get the comment length
				outCommentLength.set(text.length());

				// write out the user ID with min max dates and count
				context.write(outHour, outCommentLength);

			} catch (ParseException e) {
				System.err.println(e.getMessage());
				return;
			}
		}
	}

	public static class SOMedianStdDevReducer extends
			Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private ArrayList<Float> commentLengths = new ArrayList<Float>();

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			float sum = 0;
			float count = 0;
			commentLengths.clear();
			result.setStdDev(0);
			
			// Iterate through all input values for this key
			for (IntWritable val : values) {
				commentLengths.add((float) val.get());
				sum += val.get();
				++count;
			}

			// sort commentLengths to calculate median
			Collections.sort(commentLengths);

			// if commentLengths is an even value, average middle two elements
			if (count % 2 == 0) {
				result.setMedian((commentLengths.get((int) count / 2 - 1) + commentLengths
						.get((int) count / 2)) / 2.0f);
			} else {
				// else, set median to middle value
				result.setMedian(commentLengths.get((int) count / 2));
			}

			// calculate standard deviation
			float mean = sum / count;

			float sumOfSquares = 0.0f;
			for (Float f : commentLengths) {
				sumOfSquares += (f - mean) * (f - mean);
			}

			result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1)));

			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MedianStdDevDriver <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf,
				"StackOverflow Comment Length Median StdDev By Hour");
		job.setJarByClass(MedianStdDevDriver.class);
		job.setMapperClass(SOMedianStdDevMapper.class);
		job.setReducerClass(SOMedianStdDevReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MedianStdDevTuple.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MedianStdDevTuple implements Writable {
		private float median = 0;
		private float stddev = 0f;

		public float getMedian() {
			return median;
		}

		public void setMedian(float median) {
			this.median = median;
		}

		public float getStdDev() {
			return stddev;
		}

		public void setStdDev(float stddev) {
			this.stddev = stddev;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			median = in.readFloat();
			stddev = in.readFloat();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeFloat(median);
			out.writeFloat(stddev);
		}

		@Override
		public String toString() {
			return median + "\t" + stddev;
		}
	}
}
