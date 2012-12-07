package mrdp.ch7;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

public class RandomDataGenerationDriver {

	public static class RandomStackOverflowInputFormat extends
			InputFormat<Text, NullWritable> {

		public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
		public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";
		public static final String RANDOM_WORD_LIST = "random.generator.random.word.file";

		@Override
		public List<InputSplit> getSplits(JobContext job) throws IOException {

			// Get the number of map tasks configured for
			int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
			if (numSplits <= 0) {
				throw new IOException(NUM_MAP_TASKS + " is not set.");
			}

			// Create a number of input splits equivalent to the number of tasks
			ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
			for (int i = 0; i < numSplits; ++i) {
				splits.add(new FakeInputSplit());
			}

			return splits;
		}

		@Override
		public RecordReader<Text, NullWritable> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			// Create a new RandomStackoverflowRecordReader and initialize it
			RandomStackoverflowRecordReader rr = new RandomStackoverflowRecordReader();
			rr.initialize(split, context);
			return rr;
		}

		public static void setNumMapTasks(Job job, int i) {
			job.getConfiguration().setInt(NUM_MAP_TASKS, i);
		}

		public static void setNumRecordPerTask(Job job, int i) {
			job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
		}

		public static void setRandomWordList(Job job, Path file) {
			DistributedCache.addCacheFile(file.toUri(), job.getConfiguration());
		}

		public static class RandomStackoverflowRecordReader extends
				RecordReader<Text, NullWritable> {

			private int numRecordsToCreate = 0;
			private int createdRecords = 0;
			private Text key = new Text();
			private NullWritable value = NullWritable.get();
			private Random rndm = new Random();
			private ArrayList<String> randomWords = new ArrayList<String>();

			// This object will format the creation date string into a Date
			// object
			private SimpleDateFormat frmt = new SimpleDateFormat(
					"yyyy-MM-dd'T'HH:mm:ss.SSS");

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {

				// Get the number of records to create from the configuration
				this.numRecordsToCreate = context.getConfiguration().getInt(
						NUM_RECORDS_PER_TASK, -1);

				if (numRecordsToCreate < 0) {
					throw new InvalidParameterException(NUM_RECORDS_PER_TASK
							+ " is not set.");
				}

				// Get the list of random words from the DistributedCache
				URI[] files = DistributedCache.getCacheFiles(context
						.getConfiguration());

				if (files.length == 0) {
					throw new InvalidParameterException(
							"Random word list not set in cache.");
				} else {
					// Read the list of random words into a list
					BufferedReader rdr = new BufferedReader(new FileReader(
							files[0].toString()));

					String line;
					while ((line = rdr.readLine()) != null) {
						randomWords.add(line);
					}
					rdr.close();

					if (randomWords.size() == 0) {
						throw new IOException("Random word list is empty");
					}
				}
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				// If we still have records to create
				if (createdRecords < numRecordsToCreate) {
					// Generate random data
					int score = Math.abs(rndm.nextInt()) % 15000;
					int rowId = Math.abs(rndm.nextInt()) % 1000000000;
					int postId = Math.abs(rndm.nextInt()) % 100000000;
					int userId = Math.abs(rndm.nextInt()) % 1000000;
					String creationDate = frmt
							.format(Math.abs(rndm.nextLong()));

					// Create a string of text from the random words
					String text = getRandomText();

					String randomRecord = "<row Id=\"" + rowId + "\" PostId=\""
							+ postId + "\" Score=\"" + score + "\" Text=\""
							+ text + "\" CreationDate=\"" + creationDate
							+ "\" UserId\"=" + userId + "\" />";

					key.set(randomRecord);
					++createdRecords;
					return true;
				} else {
					// Else, return false
					return false;
				}
			}

			/**
			 * Creates a random string of words from the list. 1-30 words per
			 * string.
			 * 
			 * @return A random string of words
			 */
			private String getRandomText() {
				StringBuilder bldr = new StringBuilder();
				int numWords = Math.abs(rndm.nextInt()) % 30 + 1;

				for (int i = 0; i < numWords; ++i) {
					bldr.append(randomWords.get(Math.abs(rndm.nextInt())
							% randomWords.size())
							+ " ");
				}
				return bldr.toString();
			}

			@Override
			public Text getCurrentKey() throws IOException,
					InterruptedException {
				return key;
			}

			@Override
			public NullWritable getCurrentValue() throws IOException,
					InterruptedException {
				return value;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return (float) createdRecords / (float) numRecordsToCreate;
			}

			@Override
			public void close() throws IOException {
				// nothing to do here...
			}
		}

		/**
		 * This class is very empty.
		 */
		public static class FakeInputSplit extends InputSplit implements
				Writable {

			@Override
			public void readFields(DataInput arg0) throws IOException {
			}

			@Override
			public void write(DataOutput arg0) throws IOException {
			}

			@Override
			public long getLength() throws IOException, InterruptedException {
				return 0;
			}

			@Override
			public String[] getLocations() throws IOException,
					InterruptedException {
				return new String[0];
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err
					.println("Usage: RandomDataGenerationDriver <num map tasks> <num records per task> <word list> <output>");
			System.exit(1);
		}

		int numMapTasks = Integer.parseInt(otherArgs[0]);
		int numRecordsPerTask = Integer.parseInt(otherArgs[1]);
		Path wordList = new Path(otherArgs[2]);
		Path outputDir = new Path(otherArgs[3]);

		Job job = new Job(conf, "RandomDataGenerationDriver");
		job.setJarByClass(RandomDataGenerationDriver.class);

		job.setNumReduceTasks(0);

		job.setInputFormatClass(RandomStackOverflowInputFormat.class);

		RandomStackOverflowInputFormat.setNumMapTasks(job, numMapTasks);
		RandomStackOverflowInputFormat.setNumRecordPerTask(job,
				numRecordsPerTask);
		RandomStackOverflowInputFormat.setRandomWordList(job, wordList);

		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}
