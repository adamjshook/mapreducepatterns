package mrdp.ch7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

import redis.clients.jedis.Jedis;

public class PartitionPruningOutputDriver {

	private static final HashMap<Integer, String> MONTH_FROM_INT = new HashMap<Integer, String>();

	static {
		MONTH_FROM_INT.put(0, "JAN");
		MONTH_FROM_INT.put(1, "FEB");
		MONTH_FROM_INT.put(2, "MAR");
		MONTH_FROM_INT.put(3, "APR");
		MONTH_FROM_INT.put(4, "MAY");
		MONTH_FROM_INT.put(5, "JUN");
		MONTH_FROM_INT.put(6, "JUL");
		MONTH_FROM_INT.put(7, "AUG");
		MONTH_FROM_INT.put(8, "SEP");
		MONTH_FROM_INT.put(9, "OCT");
		MONTH_FROM_INT.put(10, "NOV");
		MONTH_FROM_INT.put(11, "DEC");
	}

	public static class RedisLastAccessOutputMapper extends
			Mapper<Object, Text, RedisKey, Text> {

		// This object will format the creation date string into a Date object
		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		private RedisKey outkey = new RedisKey();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");

			// Grab the last access date
			String strDate = parsed.get("LastAccessDate");

			if (userId == null || reputation == null || strDate == null) {
				return;
			}

			try {
				// Parse the string into a Calendar object
				Calendar cal = Calendar.getInstance();
				cal.setTime(frmt.parse(strDate));

				// Set our output key and values
				outkey.setLastAccessMonth(cal.get(Calendar.MONTH));
				outkey.setField(userId);
				outvalue.set(reputation);

				context.write(outkey, outvalue);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	public static class RedisKey implements WritableComparable<RedisKey> {

		private int lastAccessMonth = 0;
		private Text field = new Text();

		public int getLastAccessMonth() {
			return this.lastAccessMonth;
		}

		public void setLastAccessMonth(int lastAccessMonth) {
			this.lastAccessMonth = lastAccessMonth;
		}

		public Text getField() {
			return this.field;
		}

		public void setField(String field) {
			this.field.set(field);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			lastAccessMonth = in.readInt();
			this.field.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(lastAccessMonth);
			this.field.write(out);
		}

		@Override
		public int compareTo(RedisKey rhs) {
			if (this.lastAccessMonth == rhs.getLastAccessMonth()) {
				return this.field.compareTo(rhs.getField());
			} else {
				return this.lastAccessMonth < rhs.getLastAccessMonth() ? -1 : 1;
			}
		}

		@Override
		public String toString() {
			return this.lastAccessMonth + "\t" + this.field.toString();
		}

		@Override
		public int hashCode() {
			return toString().hashCode();
		}
	}

	public static class RedisLastAccessOutputFormat extends
			OutputFormat<RedisKey, Text> {

		@Override
		public RecordWriter<RedisKey, Text> getRecordWriter(
				TaskAttemptContext job) throws IOException,
				InterruptedException {
			return new RedisLastAccessRecordWriter();
		}

		@Override
		public void checkOutputSpecs(JobContext context) throws IOException,
				InterruptedException {
		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			return (new NullOutputFormat<Text, Text>())
					.getOutputCommitter(context);
		}

		public static class RedisLastAccessRecordWriter extends
				RecordWriter<RedisKey, Text> {

			private HashMap<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();

			public RedisLastAccessRecordWriter() {
				// Create a connection to Redis for each host
				int i = 0;
				for (String host : MRDPUtils.REDIS_INSTANCES) {
					Jedis jedis = new Jedis(host);
					jedis.connect();
					jedisMap.put(i, jedis);
					jedisMap.put(i + 1, jedis);
					i += 2;
				}
			}

			@Override
			public void write(RedisKey key, Text value) throws IOException,
					InterruptedException {
				// Get the Jedis instance that this key/value pair will be
				// written to -- (0,1)->0, (2-3)->1, ... , (10-11)->5
				Jedis j = jedisMap.get(key.getLastAccessMonth());

				// Write the key/value pair
				j.hset(MONTH_FROM_INT.get(key.getLastAccessMonth()), key
						.getField().toString(), value.toString());
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

		if (otherArgs.length != 1) {
			System.err.println("Usage: PartitionPruningOutput <user data>");
			System.exit(1);
		}

		Path inputPath = new Path(otherArgs[0]);

		Job job = new Job(conf, "Redis Last Access Output");
		job.setJarByClass(PartitionPruningOutputDriver.class);

		job.setMapperClass(RedisLastAccessOutputMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inputPath);

		job.setOutputFormatClass(RedisLastAccessOutputFormat.class);

		job.setOutputKeyClass(RedisKey.class);
		job.setOutputValueClass(Text.class);

		int code = job.waitForCompletion(true) ? 0 : 2;

		System.exit(code);
	}
}
