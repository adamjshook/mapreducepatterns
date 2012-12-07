package mrdp.ch7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

public class RedisInputDriver {

	public static class RedisHashInputFormat extends
			InputFormat<Text, Text> {

		public static final String REDIS_HOSTS_CONF = "mapred.redishashinputformat.hosts";
		public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";
		private static final Logger LOG = Logger
				.getLogger(RedisHashInputFormat.class);

		/**
		 * Sets the CSV string of Redis hosts.
		 * 
		 * @param job
		 *            The job conf
		 * @param hosts
		 *            The CSV string of Redis hosts
		 */
		public static void setRedisHosts(Job job, String hosts) {
			job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
		}

		/**
		 * Sets the key of the hash to write to.
		 * 
		 * @param job
		 *            The job conf
		 * @param hashKey
		 *            The name of the hash key
		 */
		public static void setRedisHashKey(Job job, String hashKey) {
			job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
		}

		@Override
		public List<InputSplit> getSplits(JobContext job) throws IOException {
			String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);

			if (hosts == null || hosts.isEmpty()) {
				throw new IOException(REDIS_HOSTS_CONF
						+ " is not set in configuration.");
			}

			String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
			if (hashKey == null || hashKey.isEmpty()) {
				throw new IOException(REDIS_HASH_KEY_CONF
						+ " is not set in configuration.");
			}

			// Create an input split for each host
			List<InputSplit> splits = new ArrayList<InputSplit>();
			for (String host : hosts.split(",")) {
				splits.add(new RedisHashInputSplit(host, hashKey));
			}

			LOG.info("Input splits to process: " + splits.size());
			return splits;
		}

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split,
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			return new RedisHashRecordReader();
		}

		public static class RedisHashRecordReader extends
				RecordReader<Text, Text> {

			private static final Logger LOG = Logger
					.getLogger(RedisHashRecordReader.class);
			private Iterator<Entry<String, String>> keyValueMapIter = null;
			private Text key = new Text(), value = new Text();
			private float processedKVs = 0, totalKVs = 0;
			private Entry<String, String> currentEntry = null;

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {

				// Get the host location from the InputSplit
				String host = split.getLocations()[0];
				String hashKey = ((RedisHashInputSplit) split).getHashKey();

				LOG.info("Connecting to " + host + " and reading from "
						+ hashKey);

				Jedis jedis = new Jedis(host);
				jedis.connect();
				jedis.getClient().setTimeoutInfinite();

				// Get all the key value pairs from the Redis instance and store
				// them in memory
				totalKVs = jedis.hlen(hashKey);
				keyValueMapIter = jedis.hgetAll(hashKey).entrySet().iterator();
				LOG.info("Got " + totalKVs + " from " + hashKey);
				jedis.disconnect();
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {

				// If the key/value map still has values
				if (keyValueMapIter.hasNext()) {

					// Get the current entry and set the Text objects to the
					// entry
					currentEntry = keyValueMapIter.next();
					key.set(currentEntry.getKey());
					value.set(currentEntry.getValue());
					return true;
				} else {
					// No more values? return false.
					return false;
				}
			}

			@Override
			public Text getCurrentKey() throws IOException,
					InterruptedException {
				return key;
			}

			@Override
			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return value;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return processedKVs / totalKVs;
			}

			@Override
			public void close() throws IOException {
				// nothing to do here
			}
		}
	}

	public static class RedisHashInputSplit extends InputSplit implements Writable {

		/**
		 * The Redis instance location
		 */
		private String location = null;

		/**
		 * The Redis hash to read from
		 */
		private String hashKey = null;

		public RedisHashInputSplit() {
			// Default constructor for reflection
		}

		public RedisHashInputSplit(String redisHost, String hash) {
			this.location = redisHost;
			this.hashKey = hash;
		}

		public String getHashKey() {
			return this.hashKey;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.location = in.readUTF();
			this.hashKey = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(location);
			out.writeUTF(hashKey);
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] { location };
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err
					.println("Usage: RedisInput <redis hosts> <hash name> <output>");
			System.exit(1);
		}

		String hosts = otherArgs[0];
		String hashKey = otherArgs[1];
		Path outputDir = new Path(otherArgs[2]);

		Job job = new Job(conf, "Redis Input");
		job.setJarByClass(RedisInputDriver.class);

		// Use the identity mapper
		job.setNumReduceTasks(0);

		job.setInputFormatClass(RedisHashInputFormat.class);
		RedisHashInputFormat.setRedisHosts(job, hosts);
		RedisHashInputFormat.setRedisHashKey(job, hashKey);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}
