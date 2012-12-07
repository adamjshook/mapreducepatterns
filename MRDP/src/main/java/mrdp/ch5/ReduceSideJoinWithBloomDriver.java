package mrdp.ch5;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class ReduceSideJoinWithBloomDriver {

	public static class UserJoinMapperWithBloom extends
			Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");

			if (userId == null || reputation == null) {
				return;
			}

			// If the reputation is greater than 1,500, output the user ID with
			// the value
			if (Integer.parseInt(reputation) > 1500) {
				outkey.set(parsed.get("Id"));
				outvalue.set("A" + value.toString());
				context.write(outkey, outvalue);
			}
		}

		public static class CommentJoinMapperWithBloom extends
				Mapper<Object, Text, Text, Text> {

			private BloomFilter bfilter = new BloomFilter();
			private Text outkey = new Text();
			private Text outvalue = new Text();

			@Override
			public void setup(Context context) {
				try {
					Path[] files = DistributedCache.getLocalCacheFiles(context
							.getConfiguration());

					if (files.length != 0) {
						DataInputStream strm = new DataInputStream(
								new FileInputStream(new File(
										files[0].toString())));
						bfilter.readFields(strm);
					} else {
						throw new RuntimeException(
								"Bloom filter not set in DistributedCache");
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
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

				if (bfilter.membershipTest(new Key(userId.getBytes()))) {
					outkey.set(userId);
					outvalue.set("B" + value.toString());
					context.write(outkey, outvalue);
				}
			}
		}

		public static class UserJoinReducer extends
				Reducer<Text, Text, Text, Text> {

			private ArrayList<Text> listA = new ArrayList<Text>();
			private ArrayList<Text> listB = new ArrayList<Text>();
			private String joinType = null;

			@Override
			public void setup(Context context) {
				// Get the type of join from our configuration
				joinType = context.getConfiguration().get("join.type");
			}

			@Override
			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {

				// Clear our lists
				listA.clear();
				listB.clear();

				// iterate through all our values, binning each record based on
				// what
				// it was tagged with
				// make sure to remove the tag!
				for (Text t : values) {
					if (t.charAt(0) == 'A') {
						listA.add(new Text(t.toString().substring(1)));
					} else /* if (tmp.charAt('0') == 'B') */{
						listB.add(new Text(t.toString().substring(1)));
					}
				}

				// Execute our join logic now that the lists are filled
				executeJoinLogic(context);
			}

			private void executeJoinLogic(Context context) throws IOException,
					InterruptedException {
				if (joinType.equalsIgnoreCase("inner")) {
					// If both lists are not empty, join A with B
					if (!listA.isEmpty() && !listB.isEmpty()) {
						for (Text A : listA) {
							for (Text B : listB) {
								context.write(A, B);
							}
						}
					}
				} else if (joinType.equalsIgnoreCase("leftouter")) {
					// For each entry in A,
					for (Text A : listA) {
						// If list B is not empty, join A and B
						if (!listB.isEmpty()) {
							for (Text B : listB) {
								context.write(A, B);
							}
						} else {
							// Else, output A by itself
							context.write(A, new Text(""));
						}
					}
				} else if (joinType.equalsIgnoreCase("rightouter")) {
					// FOr each entry in B,
					for (Text B : listB) {
						// If list A is not empty, join A and B
						if (!listA.isEmpty()) {
							for (Text A : listA) {
								context.write(A, B);
							}
						} else {
							// Else, output B by itself
							context.write(new Text(""), B);
						}
					}
				} else if (joinType.equalsIgnoreCase("fullouter")) {
					// If list A is not empty
					if (!listA.isEmpty()) {
						// For each entry in A
						for (Text A : listA) {
							// If list B is not empty, join A with B
							if (!listB.isEmpty()) {
								for (Text B : listB) {
									context.write(A, B);
								}
							} else {
								// Else, output A by itself
								context.write(A, new Text(""));
							}
						}
					} else {
						// If list A is empty, just output B
						for (Text B : listB) {
							context.write(new Text(""), B);
						}
					}
				} else if (joinType.equalsIgnoreCase("anti")) {
					// If list A is empty and B is not empty or vice versa
					if (listA.isEmpty() ^ listB.isEmpty()) {

						// Iterate both A and B with null values
						// The previous XOR check will make sure exactly one of
						// these lists is empty and therefore won't have output
						for (Text A : listA) {
							context.write(A, new Text(""));
						}

						for (Text B : listB) {
							context.write(new Text(""), B);
						}
					}
				} else {
					throw new RuntimeException(
							"Join type not set to inner, leftouter, rightouter, fullouter, or anti");
				}
			}
		}

		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			if (otherArgs.length != 4) {
				System.err
						.println("Usage: ReduceSideJoin <user data> <comment data> <out> [inner|leftouter|rightouter|fullouter|anti]");
				System.exit(1);
			}

			String joinType = otherArgs[3];
			if (!(joinType.equalsIgnoreCase("inner")
					|| joinType.equalsIgnoreCase("leftouter")
					|| joinType.equalsIgnoreCase("rightouter")
					|| joinType.equalsIgnoreCase("fullouter") || joinType
						.equalsIgnoreCase("anti"))) {
				System.err
						.println("Join type not set to inner, leftouter, rightouter, fullouter, or anti");
				System.exit(2);
			}

			Job job = new Job(conf, "Reduce Side Join");
			// Configure the join type
			job.getConfiguration().set("join.type", joinType);
			job.setJarByClass(ReduceSideJoinWithBloomDriver.class);

			// Use multiple inputs to set which input uses what mapper
			// This will keep parsing of each data set separate from a logical
			// standpoint
			MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
					TextInputFormat.class, UserJoinMapperWithBloom.class);
			MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					TextInputFormat.class, CommentJoinMapperWithBloom.class);

			job.setReducerClass(UserJoinReducer.class);

			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			System.exit(job.waitForCompletion(true) ? 0 : 3);
		}
	}
}
