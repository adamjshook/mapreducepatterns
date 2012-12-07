package mrdp.ch6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MergedJobDriver {

	public static final String MULTIPLE_OUTPUTS_ANONYMIZE = "anonymize";
	public static final String MULTIPLE_OUTPUTS_DISTINCT = "distinct";

	public static class AnonymizeDistinctMergedMapper extends
			Mapper<Object, Text, TaggedText, Text> {

		private static final Text DISTINCT_OUT_VALUE = new Text();

		private Random rndm = new Random();
		private TaggedText anonymizeOutkey = new TaggedText(),
				distinctOutkey = new TaggedText();
		private Text anonymizeOutvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			anonymizeMap(key, value, context);
			distinctMap(key, value, context);
		}

		private void anonymizeMap(Object key, Text value, Context context)
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
				anonymizeOutkey.setTag("A");
				anonymizeOutkey.setText(Integer.toString(rndm.nextInt()));
				anonymizeOutvalue.set(bldr.toString());
				context.write(anonymizeOutkey, anonymizeOutvalue);
			}
		}

		private void distinctMap(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Parse the input into a nice map.
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			// Get the value for the UserId attribute
			String userId = parsed.get("UserId");

			// If it is null, skip this record
			if (userId == null) {
				return;
			}

			// Otherwise, set our output key to the user's id, tagged with a "D"
			distinctOutkey.setTag("D");
			distinctOutkey.setText(userId);

			// Write the user's id with a null value
			context.write(distinctOutkey, DISTINCT_OUT_VALUE);
		}
	}

	public static class AnonymizeDistinctMergedReducer extends
			Reducer<TaggedText, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> mos = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}

		@Override
		protected void reduce(TaggedText key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			if (key.getTag().equals("A")) {
				anonymizeReduce(key.getText(), values, context);
			} else {
				distinctReduce(key.getText(), values, context);
			}
		}

		private void anonymizeReduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				mos.write(MULTIPLE_OUTPUTS_ANONYMIZE, value,
						NullWritable.get(), MULTIPLE_OUTPUTS_ANONYMIZE
								+ "/part");
			}
		}

		private void distinctReduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			mos.write(MULTIPLE_OUTPUTS_DISTINCT, key, NullWritable.get(),
					MULTIPLE_OUTPUTS_DISTINCT + "/part");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MergedJob <comment data> <out>");
			System.exit(1);
		}

		// Configure the merged job
		Job job = new Job(conf, "MergedJob");
		job.setJarByClass(MergedJobDriver.class);

		job.setMapperClass(AnonymizeDistinctMergedMapper.class);
		job.setReducerClass(AnonymizeDistinctMergedReducer.class);
		job.setNumReduceTasks(10);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_ANONYMIZE,
				TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_DISTINCT,
				TextOutputFormat.class, Text.class, NullWritable.class);

		job.setOutputKeyClass(TaggedText.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}

	public static class TaggedText implements WritableComparable<TaggedText> {

		private String tag = "";
		private Text text = new Text();

		public TaggedText() {

		}

		public TaggedText(TaggedText text) {
			setTag(text.getTag());
			setText(text.getText());
		}

		public void setTag(String tag) {
			this.tag = tag;
		}

		public String getTag() {
			return tag;
		}

		public void setText(Text text) {
			this.text.set(text);
		}

		public void setText(String text) {
			this.text.set(text);
		}

		public Text getText() {
			return text;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			tag = in.readUTF();
			text.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(tag);
			text.write(out);
		}

		@Override
		public int compareTo(TaggedText obj) {
			int compare = tag.compareTo(obj.getTag());
			if (compare == 0) {
				return text.compareTo(obj.getText());
			} else {
				return compare;
			}
		}
		
		@Override
		public String toString() {
			return tag.toString() + ":" + text.toString();
		}
	}
}
