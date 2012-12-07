package mrdp.ch2;

import java.io.IOException;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.lang.StringEscapeUtils;

public class WikipediaIndex {

	public static String getWikipediaURL(String text) {

		int idx = text.indexOf("\"http://en.wikipedia.org");
		if (idx == -1) {
			return null;
		}
		int idx_end = text.indexOf('"', idx + 1);

		if (idx_end == -1) {
			return null;
		}

		int idx_hash = text.indexOf('#', idx + 1);

		if (idx_hash != -1 && idx_hash < idx_end) {
			return text.substring(idx + 1, idx_hash);
		} else {
			return text.substring(idx + 1, idx_end);
		}

	}

	public static class SOWikipediaExtractor extends
			Mapper<Object, Text, Text, Text> {

		private Text link = new Text();
		private Text outkey = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			// Grab the necessary XML attributes
			String txt = parsed.get("Body");
			String posttype = parsed.get("PostTypeId");
			String row_id = parsed.get("Id");

			// if the body is null, or the post is a question (1), skip
			if (txt == null || (posttype != null && posttype.equals("1"))) {
				return;
			}

			// Unescape the HTML because the SO data is escaped.
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

			link.set(getWikipediaURL(txt));
			outkey.set(row_id);
			context.write(link, outkey);
		}
	}

	public static class Concatenator extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (Text id : values) {
				sb.append(id.toString() + " ");
			}

			result.set(sb.substring(0, sb.length() - 1).toString());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: WikipediallIndex <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "StackOverflow Wikipedia URL Inverted Index");
		job.setJarByClass(WikipediaIndex.class);
		job.setMapperClass(SOWikipediaExtractor.class);
		job.setCombinerClass(Concatenator.class);
		job.setReducerClass(Concatenator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
