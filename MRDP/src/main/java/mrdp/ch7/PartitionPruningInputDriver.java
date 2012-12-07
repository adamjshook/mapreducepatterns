package mrdp.ch7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import mrdp.ch7.PartitionPruningOutputDriver.RedisKey;
import mrdp.utils.MRDPUtils;

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

public class PartitionPruningInputDriver {

	public static class RedisLastAccessInputFormat extends
			InputFormat<RedisKey, Text> {

		public static final String REDIS_SELECTED_MONTHS_CONF = "mapred.redilastaccessinputformat.months";
		private static final HashMap<String, Integer> MONTH_FROM_STRING = new HashMap<String, Integer>();
		private static final HashMap<String, String> MONTH_TO_INST_MAP = new HashMap<String, String>();
		private static final Logger LOG = Logger
				.getLogger(RedisLastAccessInputFormat.class);

		static {
			MONTH_TO_INST_MAP.put("JAN", MRDPUtils.REDIS_INSTANCES[0]);
			MONTH_TO_INST_MAP.put("FEB", MRDPUtils.REDIS_INSTANCES[0]);
			MONTH_TO_INST_MAP.put("MAR", MRDPUtils.REDIS_INSTANCES[1]);
			MONTH_TO_INST_MAP.put("APR", MRDPUtils.REDIS_INSTANCES[1]);
			MONTH_TO_INST_MAP.put("MAY", MRDPUtils.REDIS_INSTANCES[2]);
			MONTH_TO_INST_MAP.put("JUN", MRDPUtils.REDIS_INSTANCES[2]);
			MONTH_TO_INST_MAP.put("JUL", MRDPUtils.REDIS_INSTANCES[3]);
			MONTH_TO_INST_MAP.put("AUG", MRDPUtils.REDIS_INSTANCES[3]);
			MONTH_TO_INST_MAP.put("SEP", MRDPUtils.REDIS_INSTANCES[4]);
			MONTH_TO_INST_MAP.put("OCT", MRDPUtils.REDIS_INSTANCES[4]);
			MONTH_TO_INST_MAP.put("NOV", MRDPUtils.REDIS_INSTANCES[5]);
			MONTH_TO_INST_MAP.put("DEC", MRDPUtils.REDIS_INSTANCES[5]);

			MONTH_FROM_STRING.put("JAN", 0);
			MONTH_FROM_STRING.put("FEB", 1);
			MONTH_FROM_STRING.put("MAR", 2);
			MONTH_FROM_STRING.put("APR", 3);
			MONTH_FROM_STRING.put("MAY", 4);
			MONTH_FROM_STRING.put("JUN", 5);
			MONTH_FROM_STRING.put("JUL", 6);
			MONTH_FROM_STRING.put("AUG", 7);
			MONTH_FROM_STRING.put("SEP", 8);
			MONTH_FROM_STRING.put("OCT", 9);
			MONTH_FROM_STRING.put("NOV", 10);
			MONTH_FROM_STRING.put("DEC", 11);
		}

		/**
		 * Sets the CSV string for months you want to pull
		 * 
		 * @param job
		 *            The job conf
		 * @param String
		 *            months The CSV list of months
		 */
		public static void setRedisLastAccessMonths(Job job, String months) {
			job.getConfiguration().set(REDIS_SELECTED_MONTHS_CONF, months);
		}

		@Override
		public List<InputSplit> getSplits(JobContext job) throws IOException {

			String months = job.getConfiguration().get(
					REDIS_SELECTED_MONTHS_CONF);

			if (months == null || months.isEmpty()) {
				throw new IOException(REDIS_SELECTED_MONTHS_CONF
						+ " is null or empty.");
			}

			// Create input splits from the input months
			HashMap<String, RedisLastAccessInputSplit> instanceToSplitMap = new HashMap<String, RedisLastAccessInputSplit>();
			for (String month : months.split(",")) {
				String host = MONTH_TO_INST_MAP.get(month);
				RedisLastAccessInputSplit split = instanceToSplitMap.get(host);
				if (split == null) {
					split = new RedisLastAccessInputSplit(host);
					split.addHashKey(month);
					instanceToSplitMap.put(host, split);
				} else {
					split.addHashKey(month);
				}
			}

			LOG.info("Input splits to process: "
					+ instanceToSplitMap.values().size());
			return new ArrayList<InputSplit>(instanceToSplitMap.values());
		}

		@Override
		public RecordReader<RedisKey, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new RedisLastAccessRecordReader();
		}

		public static class RedisLastAccessRecordReader extends
				RecordReader<RedisKey, Text> {

			private static final Logger LOG = Logger
					.getLogger(RedisLastAccessRecordReader.class);
			private Entry<String, String> currentEntry = null;
			private float processedKVs = 0, totalKVs = 0;
			private int currentHashMonth = 0;
			private Iterator<Entry<String, String>> hashIterator = null;
			private Iterator<String> hashKeys = null;
			private RedisKey key = new RedisKey();
			private String host = null;
			private Text value = new Text();

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {

				// Get the host location from the InputSplit
				host = split.getLocations()[0];

				// Get an iterator of all the hash keys we want to read
				hashKeys = ((RedisLastAccessInputSplit) split).getHashKeys()
						.iterator();

				LOG.info("Connecting to " + host);
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {

				boolean nextHashKey = false;
				do {
					// if this is the first call or the iterator does not have a
					// next
					if (hashIterator == null || !hashIterator.hasNext()) {
						// if we have reached the end of our hash keys, return
						// false
						if (!hashKeys.hasNext()) {
							// ultimate end condition, return false
							return false;
						} else {
							// Otherwise, connect to Redis and get all
							// the name/value pairs for this hash key
							Jedis jedis = new Jedis(host);
							jedis.connect();
							String strKey = hashKeys.next();
							currentHashMonth = MONTH_FROM_STRING.get(strKey);
							hashIterator = jedis.hgetAll(strKey).entrySet()
									.iterator();
							jedis.disconnect();
						}
					}

					// If the key/value map still has values
					if (hashIterator.hasNext()) {
						// Get the current entry and set the Text objects to
						// the
						// entry
						currentEntry = hashIterator.next();
						key.setLastAccessMonth(currentHashMonth);
						key.setField(currentEntry.getKey());
						value.set(currentEntry.getValue());
					} else {
						nextHashKey = true;
					}
				} while (nextHashKey);

				return true;
			}

			@Override
			public RedisKey getCurrentKey() throws IOException,
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

	public static class RedisLastAccessInputSplit extends InputSplit implements
			Writable {

		/**
		 * The Redis instance location
		 */
		private String location = null;
		private List<String> hashKeys = new ArrayList<String>();

		public RedisLastAccessInputSplit() {
			// Default constructor for reflection
		}

		public RedisLastAccessInputSplit(String redisHost) {
			this.location = redisHost;
		}

		public void addHashKey(String key) {
			hashKeys.add(key);
		}

		public void removeHashKey(String key) {
			hashKeys.remove(key);
		}

		public List<String> getHashKeys() {
			return hashKeys;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			location = in.readUTF();
			int numKeys = in.readInt();
			hashKeys.clear();
			for (int i = 0; i < numKeys; ++i) {
				hashKeys.add(in.readUTF());
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(location);
			out.writeInt(hashKeys.size());
			for (String key : hashKeys) {
				out.writeUTF(key);
			}
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

		if (otherArgs.length != 2) {
			System.err
					.println("Usage: PartitionPruning <last access months> <output>");
			System.exit(1);
		}

		String lastAccessMonths = otherArgs[0];
		Path outputDir = new Path(otherArgs[1]);

		Job job = new Job(conf, "Redis Input");
		job.setJarByClass(PartitionPruningInputDriver.class);

		// Use the identity mapper
		job.setNumReduceTasks(0);

		job.setInputFormatClass(RedisLastAccessInputFormat.class);
		RedisLastAccessInputFormat.setRedisLastAccessMonths(job,
				lastAccessMonths);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(RedisKey.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}
