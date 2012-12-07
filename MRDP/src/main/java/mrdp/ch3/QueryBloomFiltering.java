package mrdp.ch3;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class QueryBloomFiltering {

	public static class BloomFilteringMapper extends
			Mapper<Object, Text, Text, NullWritable> {

		private BloomFilter filter = new BloomFilter();
		private HTable table = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			URI[] files = DistributedCache.getCacheFiles(context
					.getConfiguration());

			// if the files in the distributed cache are set
			if (files != null && files.length == 1) {
				System.out.println("Reading Bloom filter from: "
						+ files[0].getPath());

				// Open local file for read.
				DataInputStream strm = new DataInputStream(new FileInputStream(
						files[0].getPath()));

				// Read into our Bloom filter.
				filter.readFields(strm);
				strm.close();
			} else {
				throw new IOException(
						"Bloom filter file not set in the DistributedCache.");
			}

			// Get HBase table of user info
			Configuration hconf = HBaseConfiguration.create();
			table = new HTable(hconf, "user_table");
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input into a nice map.
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			// Get the value for the comment
			String userid = parsed.get("UserId");

			// If it is null, skip this record
			if (userid == null) {
				return;
			}

			// If this user ID is in the set
			if (filter.membershipTest(new Key(userid.getBytes()))) {
				// Get the reputation from the HBase table
				Result r = table.get(new Get(userid.getBytes()));
				int reputation = Integer.parseInt(new String(r.getValue(
						"attr".getBytes(), "Reputation".getBytes())));
				// If the reputation is at least 1,500,
				// write the record to the file system
				if (reputation >= 1500) {
					context.write(value, NullWritable.get());
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: BloomFiltering <in> <cachefile> <out>");
			System.exit(1);
		}

		FileSystem.get(conf).delete(new Path(otherArgs[2]), true);

		Job job = new Job(conf, "StackOverflow Bloom Filtering");
		job.setJarByClass(QueryBloomFiltering.class);
		job.setMapperClass(BloomFilteringMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		DistributedCache.addCacheFile(
				FileSystem.get(conf).makeQualified(new Path(otherArgs[1]))
						.toUri(), job.getConfiguration());

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
