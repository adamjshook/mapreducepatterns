package mrdp.ch6;

import java.io.IOException;
import mrdp.ch6.JobChainingDriver.UserIdBinningMapper;
import mrdp.ch6.JobChainingDriver.UserIdCountMapper;
import mrdp.ch6.JobChainingDriver.UserIdSumReducer;
import mrdp.ch6.ParallelJobs.AverageReputationMapper;
import mrdp.ch6.ParallelJobs.AverageReputationReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class JobControlDriver {
	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.err
					.println("Usage: JobChainingDriver <posts> <users> <belowavgrepout> <aboveavgrepout>");
			System.exit(2);
		}

		Path postInput = new Path(args[0]);
		Path userInput = new Path(args[1]);
		Path countingOutput = new Path(args[3] + "_count");
		Path binningOutputRoot = new Path(args[3] + "_bins");
		Path binningOutputBelow = new Path(binningOutputRoot + "/"
				+ JobChainingDriver.MULTIPLE_OUTPUTS_BELOW_NAME);
		Path binningOutputAbove = new Path(binningOutputRoot + "/"
				+ JobChainingDriver.MULTIPLE_OUTPUTS_ABOVE_NAME);

		Path belowAverageRepOutput = new Path(args[2]);
		Path aboveAverageRepOutput = new Path(args[3]);

		Job countingJob = getCountingJob(postInput, countingOutput);

		int code = 1;
		if (countingJob.waitForCompletion(true)) {
			ControlledJob binningControlledJob = new ControlledJob(
					getBinningJobConf(countingJob, countingOutput, userInput,
							binningOutputRoot));

			ControlledJob belowAvgControlledJob = new ControlledJob(
					getAverageJobConf(binningOutputBelow, belowAverageRepOutput));
			belowAvgControlledJob.addDependingJob(binningControlledJob);

			ControlledJob aboveAvgControlledJob = new ControlledJob(
					getAverageJobConf(binningOutputAbove, aboveAverageRepOutput));
			aboveAvgControlledJob.addDependingJob(binningControlledJob);

			JobControl jc = new JobControl("AverageReputation");
			jc.addJob(binningControlledJob);
			jc.addJob(belowAvgControlledJob);
			jc.addJob(aboveAvgControlledJob);

			jc.run();
			code = jc.getFailedJobList().size() == 0 ? 0 : 1;
		}

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(countingOutput, true);
		fs.delete(binningOutputRoot, true);

		System.out.println("All Done");
		System.exit(code);
	}

	public static Job getCountingJob(Path postInput, Path outputDirIntermediate)
			throws IOException {
		// Setup first job to counter user posts
		Job countingJob = new Job(new Configuration(), "JobChaining-Counting");
		countingJob.setJarByClass(JobChainingDriver.class);

		// Set our mapper and reducer, we can use the API's long sum reducer for
		// a combiner!
		countingJob.setMapperClass(UserIdCountMapper.class);
		countingJob.setCombinerClass(LongSumReducer.class);
		countingJob.setReducerClass(UserIdSumReducer.class);

		countingJob.setOutputKeyClass(Text.class);
		countingJob.setOutputValueClass(LongWritable.class);

		countingJob.setInputFormatClass(TextInputFormat.class);

		TextInputFormat.addInputPath(countingJob, postInput);

		countingJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(countingJob, outputDirIntermediate);

		return countingJob;
	}

	public static Configuration getBinningJobConf(Job countingJob,
			Path jobchainOutdir, Path userInput, Path binningOutput)
			throws IOException {
		// Calculate the average posts per user by getting counter values
		double numRecords = (double) countingJob
				.getCounters()
				.findCounter(JobChainingDriver.AVERAGE_CALC_GROUP,
						UserIdCountMapper.RECORDS_COUNTER_NAME).getValue();
		double numUsers = (double) countingJob
				.getCounters()
				.findCounter(JobChainingDriver.AVERAGE_CALC_GROUP,
						UserIdSumReducer.USERS_COUNTER_NAME).getValue();

		double averagePostsPerUser = numRecords / numUsers;

		// Setup binning job
		Job binningJob = new Job(new Configuration(), "JobChaining-Binning");
		binningJob.setJarByClass(JobChainingDriver.class);

		// Set mapper and the average posts per user
		binningJob.setMapperClass(UserIdBinningMapper.class);
		UserIdBinningMapper.setAveragePostsPerUser(binningJob,
				averagePostsPerUser);

		binningJob.setNumReduceTasks(0);

		binningJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(binningJob, jobchainOutdir);

		// Add two named outputs for below/above average
		MultipleOutputs.addNamedOutput(binningJob,
				JobChainingDriver.MULTIPLE_OUTPUTS_BELOW_NAME,
				TextOutputFormat.class, Text.class, Text.class);

		MultipleOutputs.addNamedOutput(binningJob,
				JobChainingDriver.MULTIPLE_OUTPUTS_ABOVE_NAME,
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.setCountersEnabled(binningJob, true);

		TextOutputFormat.setOutputPath(binningJob, binningOutput);

		// Add the user files to the DistributedCache
		FileStatus[] userFiles = FileSystem.get(new Configuration())
				.listStatus(userInput);
		for (FileStatus status : userFiles) {
			DistributedCache.addCacheFile(status.getPath().toUri(),
					binningJob.getConfiguration());
		}

		// Execute job and grab exit code
		return binningJob.getConfiguration();
	}

	public static Configuration getAverageJobConf(Path averageOutputDir,
			Path outputDir) throws IOException {

		Job averageJob = new Job(new Configuration(), "ParallelJobs");
		averageJob.setJarByClass(ParallelJobs.class);

		averageJob.setMapperClass(AverageReputationMapper.class);
		averageJob.setReducerClass(AverageReputationReducer.class);

		averageJob.setOutputKeyClass(Text.class);
		averageJob.setOutputValueClass(DoubleWritable.class);

		averageJob.setInputFormatClass(TextInputFormat.class);

		TextInputFormat.addInputPath(averageJob, averageOutputDir);

		averageJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(averageJob, outputDir);

		// Execute job and grab exit code
		return averageJob.getConfiguration();
	}

}
