package mrdp.ch7;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

public class RedisOutputDriver {

	public static class RedisOutputMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");

			if (userId == null || reputation == null) {
				return;
			}

			// Set our output key and values
			outkey.set(userId);
			outvalue.set(reputation);

			context.write(outkey, outvalue);
		}
	}

	public static class RedisHashOutputFormat extends OutputFormat<Text, Text> {

		public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
		public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";

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
		public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
				throws IOException, InterruptedException {
			return new RedisHashRecordWriter(job.getConfiguration().get(
					REDIS_HASH_KEY_CONF), job.getConfiguration().get(
					REDIS_HOSTS_CONF));
		}

		@Override
		public void checkOutputSpecs(JobContext job)
				throws IOException {
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
		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			return (new NullOutputFormat<Text, Text>())
					.getOutputCommitter(context);
		}

		public static class RedisHashRecordWriter extends
				RecordWriter<Text, Text> {

			private static final Logger LOG = Logger
					.getLogger(RedisHashRecordWriter.class);
			private HashMap<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();
			private String hashKey = null;

			public RedisHashRecordWriter(String hashKey, String hosts) {
				LOG.info("Connecting to " + hosts + " and writing to "
						+ hashKey);
				this.hashKey = hashKey;
				// Create a connection to Redis for each host
				// Map an integer 0-(numRedisInstances - 1) to the instance
				int i = 0;
				for (String host : hosts.split(",")) {
					Jedis jedis = new Jedis(host);
					jedis.connect();
					jedisMap.put(i, jedis);
					++i;
				}
			}

			@Override
			public void write(Text key, Text value) throws IOException,
					InterruptedException {
				// Get the Jedis instance that this key/value pair will be
				// written to
				Jedis j = jedisMap.get(Math.abs(key.hashCode())
						% jedisMap.size());

				// Write the key/value pair
				j.hset(hashKey, key.toString(), value.toString());
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException,
					InterruptedException {
				// For each jedis instance, disconnect it
				for (Jedis jedis : jedisMap.values()) {
					jedis.disconnect();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err
					.println("Usage: RedisOutput <user data> <redis hosts> <hash name>");
			System.exit(1);
		}

		Path inputPath = new Path(otherArgs[0]);
		String hosts = otherArgs[1];
		String hashName = otherArgs[2];

		Job job = new Job(conf, "Redis Output");
		job.setJarByClass(RedisOutputDriver.class);

		job.setMapperClass(RedisOutputMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inputPath);

		job.setOutputFormatClass(RedisHashOutputFormat.class);
		RedisHashOutputFormat.setRedisHosts(job, hosts);
		RedisHashOutputFormat.setRedisHashKey(job, hashName);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		int code = job.waitForCompletion(true) ? 0 : 2;

		System.exit(code);
	}
}
