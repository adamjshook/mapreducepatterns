package mrdp.ch4;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AnonymizeDriver {

	public static class AnonymizeMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		private IntWritable outkey = new IntWritable();
		private Random rndm = new Random();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			if (parsed.size() > 0) {
				StringBuilder bldr = new StringBuilder();
				bldr.append("<row ");
				for (Entry<String, String> entry : parsed.entrySet()) {

					if (entry.getKey().equals("UserId")
							|| entry.getKey().equals("Id")) {
						// ignore these fields
					} else if (entry.getKey().equals("CreationDate")) {
						// Strip out the time, anything after the 'T' in the
						// value
						bldr.append(entry.getKey()
								+ "=\""
								+ entry.getValue().substring(0,
										entry.getValue().indexOf('T')) + "\" ");
					} else {
						// Otherwise, output this.
						bldr.append(entry.getKey() + "=\"" + entry.getValue()
								+ "\" ");
					}

				}
				bldr.append(">");
				outkey.set(rndm.nextInt());
				outvalue.set(bldr.toString());
				context.write(outkey, outvalue);
			}
		}
	}

	public static class ValueReducer extends
			Reducer<IntWritable, Text, Text, NullWritable> {
		@Override
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
			System.err.println("Usage: Anonymize <user data> <out>");
			System.exit(1);
		}

		// Configure the join type
		Job job = new Job(conf, "Anonymize");
		job.setJarByClass(AnonymizeDriver.class);

		job.setMapperClass(AnonymizeMapper.class);
		job.setReducerClass(ValueReducer.class);
		job.setNumReduceTasks(10);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
