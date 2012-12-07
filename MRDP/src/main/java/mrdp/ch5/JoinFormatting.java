package mrdp.ch5;

import java.io.IOException;
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

public class JoinFormatting {

	public static class ReplicatedJoinMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			String userId = parsed.get("UserId");

			if (userId == null) {
				return;
			}

			outkey.set(userId);
			context.write(outkey, value);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ReplicatedJoin <user data> <out>");
			System.exit(1);
		}

		// Configure the join type
		Job job = new Job(conf, "Replicated Join");
		job.setJarByClass(ReplicatedJoinDriver.class);

		job.setMapperClass(ReplicatedJoinMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
