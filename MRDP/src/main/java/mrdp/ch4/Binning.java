package mrdp.ch4;

import java.io.IOException;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Binning {

	public static class BinningMapper extends
			Mapper<Object, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> mos = null;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		protected void setup(Context context) {
			// Create a new MultipleOutputs using the context object
			mos = new MultipleOutputs(context);
		}

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			String rawtags = parsed.get("Tags");
			if (rawtags == null) {
				return;
			}

			// Tags are delimited by ><. i.e. <tag1><tag2><tag3>
			String[] tagTokens = StringEscapeUtils.unescapeHtml(rawtags).split(
					"><");

			// For each tag
			for (String tag : tagTokens) {
				// Remove any > or < from the token
				String groomed = tag.replaceAll(">|<", "").toLowerCase();

				// If this tag is one of the following, write to the named bin
				if (groomed.equalsIgnoreCase("hadoop")) {
					mos.write("bins", value, NullWritable.get(), "hadoop-tag");
				}

				if (groomed.equalsIgnoreCase("pig")) {
					mos.write("bins", value, NullWritable.get(), "pig-tag");
				}

				if (groomed.equalsIgnoreCase("hive")) {
					mos.write("bins", value, NullWritable.get(), "hive-tag");
				}

				if (groomed.equalsIgnoreCase("hbase")) {
					mos.write("bins", value, NullWritable.get(), "hbase-tag");
				}
			}

			// Get the body of the post
			String post = parsed.get("Body");

			if (post == null) {
				return;
			}

			// If the post contains the word "hadoop", write it to its own bin
			if (post.toLowerCase().contains("hadoop")) {
				mos.write("bins", value, NullWritable.get(), "hadoop-post");
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Close multiple outputs!
			mos.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Binning <posts> <outdir>");
			System.exit(1);
		}

		Job job = new Job(conf, "Binning");
		job.setJarByClass(Binning.class);
		job.setMapperClass(BinningMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Configure the MultipleOutputs by adding an output called "bins"
		// With the proper output format and mapper key/value pairs
		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
				Text.class, NullWritable.class);

		// Enable the counters for the job
		// If there is a significant number of different named outputs, this
		// should be disabled
		MultipleOutputs.setCountersEnabled(job, true);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}
