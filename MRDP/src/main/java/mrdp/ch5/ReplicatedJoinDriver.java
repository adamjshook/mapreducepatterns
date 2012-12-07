package mrdp.ch5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReplicatedJoinDriver {

	public static class ReplicatedJoinMapper extends
			Mapper<Object, Text, Text, Text> {

		private HashMap<String, String> userIdToInfo = new HashMap<String, String>();

		private Text outvalue = new Text();
		private String joinType = null;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			try {
				Path[] files = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());

				if (files == null || files.length == 0) {
					throw new RuntimeException(
							"User information is not set in DistributedCache");
				}

				// Read all files in the DistributedCache
				for (Path p : files) {
					BufferedReader rdr = new BufferedReader(
							new InputStreamReader(
									new GZIPInputStream(new FileInputStream(
											new File(p.toString())))));

					String line;
					// For each record in the user file
					while ((line = rdr.readLine()) != null) {

						// Get the user ID for this record
						Map<String, String> parsed = MRDPUtils
								.transformXmlToMap(line);
						String userId = parsed.get("Id");

						if (userId != null) {
							// Map the user ID to the record
							userIdToInfo.put(userId, line);
						}
					}
				}

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			// Get the join type
			joinType = context.getConfiguration().get("join.type");
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			String userId = parsed.get("UserId");

			if (userId == null) {
				return;
			}

			String userInformation = userIdToInfo.get(userId);

			// If the user information is not null, then output
			if (userInformation != null) {
				outvalue.set(userInformation);
				context.write(value, outvalue);
			} else if (joinType.equalsIgnoreCase("leftouter")) {
				// If we are doing a left outer join, output the record with an
				// empty value
				context.write(value, new Text(""));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err
					.println("Usage: ReplicatedJoin <user data> <comment data> <out> [inner|leftouter]");
			System.exit(1);
		}

		String joinType = otherArgs[3];
		if (!(joinType.equalsIgnoreCase("inner") || joinType
				.equalsIgnoreCase("leftouter"))) {
			System.err.println("Join type not set to inner or leftouter");
			System.exit(2);
		}

		// Configure the join type
		Job job = new Job(conf, "Replicated Join");
		job.getConfiguration().set("join.type", joinType);
		job.setJarByClass(ReplicatedJoinDriver.class);

		job.setMapperClass(ReplicatedJoinMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[1]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Configure the DistributedCache
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(),
				job.getConfiguration());

		DistributedCache.setLocalFiles(job.getConfiguration(), otherArgs[0]);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
