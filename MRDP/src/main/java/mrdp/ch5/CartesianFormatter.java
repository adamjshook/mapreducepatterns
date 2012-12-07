package mrdp.ch5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CartesianFormatter {

	public static class CommentMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text(), outvalue = new Text();
		private HashSet<String> commonWords = new HashSet<String>();

		protected void setup(Context context) throws IOException,
				InterruptedException {

			File f = new File(System.getProperty("user.dir")
					+ "/commonwords.txt");

			BufferedReader rdr = new BufferedReader(new FileReader(f));

			String word = null;
			while ((word = rdr.readLine()) != null) {
				commonWords.add(word);
			}

			rdr.close();
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			String id = parsed.get("Id");
			String comment = parsed.get("Text");

			if (id == null || comment == null) {
				return;
			}

			String[] tokens = comment.toLowerCase()
					.replaceAll("[^a-z0-9\\s]", "").split("\\s");

			HashSet<String> setTokens = new HashSet<String>(
					Arrays.asList(tokens));
			setTokens.removeAll(commonWords);

			StringBuilder bldr = new StringBuilder();

			for (String word : setTokens) {
				if (!word.isEmpty()) {
					bldr.append(word + ",");
				}
			}

			if (bldr.length() > 0) {
				outkey.set(id);
				outvalue.set(bldr.deleteCharAt(bldr.length() - 1).toString());
				context.write(outkey, outvalue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CartesianFormatter <user data> <out>");
			System.exit(1);
		}

		// Configure the join type
		Job job = new Job(conf, "CartesianFormatter");
		job.setJarByClass(CartesianFormatter.class);

		job.setMapperClass(CommentMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
