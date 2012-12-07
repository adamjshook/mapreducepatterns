package mrdp.ch5;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import mrdp.ch5.CartesianProduct.CartesianInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CartesianProductTest {

	public static void main(String[] args) throws IOException,
			InterruptedException {

		File aDir = new File(System.getProperty("user.dir") + "/A");
		aDir.mkdirs();
		File bDir = new File(System.getProperty("user.dir") + "/B");
		bDir.mkdirs();

		File a1 = new File(System.getProperty("user.dir") + "/A/A1.txt");
		a1.createNewFile();

		PrintWriter wrtr = new PrintWriter(a1);

		wrtr.println("A11");
		wrtr.println("A12");
		wrtr.println("A13");
		wrtr.println("A14");

		wrtr.flush();
		wrtr.close();

		File a2 = new File(System.getProperty("user.dir") + "/A/A2.txt");
		a2.createNewFile();

		wrtr = new PrintWriter(a2);

		wrtr.println("A21");
		wrtr.println("A22");
		wrtr.println("A23");
		wrtr.println("A24");

		wrtr.flush();
		wrtr.close();

		File b1 = new File(System.getProperty("user.dir") + "/B/B1.txt");
		b1.createNewFile();

		wrtr = new PrintWriter(b1);

		wrtr.println("B11");
		wrtr.println("B12");
		wrtr.println("B13");
		wrtr.println("B14");

		wrtr.flush();
		wrtr.close();

		File b2 = new File(System.getProperty("user.dir") + "/B/B2.txt");
		b2.createNewFile();

		wrtr = new PrintWriter(b2);

		wrtr.println("B21");
		wrtr.println("B22");
		wrtr.println("B23");
		wrtr.println("B24");

		wrtr.flush();
		wrtr.close();

		long start = System.currentTimeMillis();

		// Configure the join type
		JobConf job = new JobConf("Cartesian Product");
		job.setJarByClass(CartesianProduct.class);

		job.setMapperClass(CartesianMapper.class);

		job.setNumReduceTasks(0);

		job.setInputFormat(CartesianInputFormat.class);
		CartesianInputFormat.setLeftInputInfo(job, TextInputFormat.class,
				System.getProperty("user.dir") + "/A");
		CartesianInputFormat.setRightInputInfo(job, TextInputFormat.class,
				System.getProperty("user.dir") + "/B");

		TextOutputFormat.setOutputPath(job, new Path("cartoutputttest"));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		RunningJob jerb = JobClient.runJob(job);
		while (!jerb.isComplete()) {
			Thread.sleep(1000);
		}

		long finish = System.currentTimeMillis();

		System.out.println("Time in ms: " + (finish - start));

		System.exit(jerb.isSuccessful() ? 0 : 2);
	}

	public static class CartesianMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text arg0, Text arg1, OutputCollector<Text, Text> arg2,
				Reporter arg3) throws IOException {
			arg2.collect(arg0, arg1);
			System.out.println(arg0 + "\t" + arg1);
		}
	}
}
