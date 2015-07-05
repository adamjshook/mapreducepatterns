package com.oreilly.mrdp.ch4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oreilly.mrdp.utils.MRDPUtils;

public class SecondarySort extends Configured implements Tool {

	public static class UserDateWritable implements
			WritableComparable<UserDateWritable> {

		private Text user = new Text();
		private Date date = new Date();

		public Text getUser() {
			return user;
		}

		public void setUser(Text text) {
			this.user.set(text);
		}

		public void setDate(Date date) {
			this.date.setTime(date.getTime());
		}

		public Date getDate() {
			return date;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			user.readFields(in);
			date.setTime(in.readLong());
		}

		@Override
		public void write(DataOutput out) throws IOException {
			user.write(out);
			out.writeLong(date.getTime());
		}

		@Override
		public int compareTo(UserDateWritable o) {
			// First, compare the user IDs together
			int c = this.getUser().compareTo(o.getUser());

			// If the user IDs are equal, then compare the dates
			if (c == 0) {
				c = this.getDate().compareTo(o.getDate());
			}

			return c;
		}
	}

	public static class IdDateWritable implements Writable {

		public long id = 0;
		public Date date = new Date();

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public Date getDate() {
			return date;
		}

		public void setDate(Date date) {
			this.date.setTime(date.getTime());
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			id = in.readLong();
			date.setTime(in.readLong());
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(id);
			out.writeLong(date.getTime());
		}
	}

	public static class SessionWritable implements
			WritableComparable<SessionWritable> {

		private String user = new String();
		private long sessionId = 0;
		private List<Long> posts = new ArrayList<Long>();
		private Date start = new Date();
		private Date end = new Date();

		public String getUser() {
			return user;
		}

		public void setUser(String user) {
			this.user = user;
		}

		public long getSessionId() {
			return sessionId;
		}

		public void setSessionId(long sessionId) {
			this.sessionId = sessionId;
		}

		public List<Long> getPosts() {
			return posts;
		}

		public void addPost(long id) {
			this.posts.add(id);
		}

		public void setPosts(List<Long> posts) {
			this.posts = posts;
		}

		public Date getStart() {
			return start;
		}

		public void setStart(Date start) {
			this.start.setTime(start.getTime());
		}

		public Date getEnd() {
			return end;
		}

		public void setEnd(Date end) {
			this.end.setTime(end.getTime());
		}

		public int getNumPosts() {
			return this.posts.size();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			user = in.readUTF();
			sessionId = in.readLong();

			int numPosts = in.readInt();

			posts.clear();
			for (int i = 0; i < numPosts; ++i) {
				posts.add(in.readLong());
			}

			start.setTime(in.readLong());
			end.setTime(in.readLong());
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(user);
			out.writeLong(sessionId);
			out.writeInt(posts.size());

			for (Long p : posts) {
				out.writeLong(p);
			}

			out.writeLong(start.getTime());
			out.writeLong(end.getTime());
		}

		@Override
		public int compareTo(SessionWritable o) {
			return Long.compare(sessionId, o.sessionId);
		}

		@Override
		public int hashCode() {
			return Long.valueOf(sessionId).hashCode();
		}

		public String toString() {
			return user + "\t" + sessionId + "\t" + (start.getTime() / 1000)
					+ "\t" + (end.getTime() / 1000) + "\t" + this.posts.size()
					+ "\t" + StringUtils.join(this.posts, ",");
		}
	}

	public static class UserDateMapper extends
			Mapper<LongWritable, Text, UserDateWritable, IdDateWritable> {

		// This object will format the creation date string into a Date object
		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		private UserDateWritable outkey = new UserDateWritable();
		private IdDateWritable outvalue = new IdDateWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
					.toString());

			String postId = parsed.get("Id");
			String userId = parsed.get("OwnerUserId");
			String date = parsed.get("CreationDate");

			// make sure all fields are set and the user ID is not -1
			// (undefined)
			if (postId != null && userId != null && userId != "-1"
					&& date != null) {

				// Set the key's user ID
				outkey.getUser().set(userId);

				// Set the value's post ID
				outvalue.setId(Long.parseLong(postId));

				try {
					// Set the date to both the
					Date d = frmt.parse(date);
					outkey.setDate(d);
					outvalue.setDate(d);
				} catch (ParseException e) {
					context.getCounter("Exceptions", "Date ParseException")
							.increment(1);
					return;
				}

				context.write(outkey, outvalue);
			}
		}
	}

	public static class SessionBuilderReducer
			extends
			Reducer<UserDateWritable, IdDateWritable, SessionWritable, NullWritable> {

		private SessionWritable outkey = new SessionWritable();

		@Override
		public void reduce(UserDateWritable key,
				Iterable<IdDateWritable> values, Context context)
				throws IOException, InterruptedException {

			long sessionId = 0;

			// Set our output key to the user ID
			outkey.setUser(key.getUser().toString());

			Date previous = null;

			// Iterate through our values, which is sorted by timestamp due to
			// the secondary sort
			for (IdDateWritable t : values) {
				if (previous == null) {
					// set our initial date and the ID
					previous = new Date(t.getDate().getTime());

					outkey.setSessionId(sessionId++);
					outkey.setStart(t.getDate());
					outkey.setEnd(t.getDate());
					outkey.getPosts().clear();
					outkey.addPost(t.getId());
				} else {
					// compare last time to this time
					if (t.getDate().getTime() - previous.getTime() < (30 * 60 * 1000)) {
						outkey.addPost(t.getId());

						// just keep pushing back the end of this session
						outkey.setEnd(t.getDate());
					} else {
						// then we will consider this new time the beginning of
						// a new session

						// output this session
						context.write(outkey, NullWritable.get());

						// reset our session
						outkey.setSessionId(sessionId++);
						outkey.setStart(t.getDate());
						outkey.setEnd(t.getDate());
						outkey.getPosts().clear();
						outkey.addPost(t.getId());
					}

					// set the previous time to current
					previous.setTime(t.getDate().getTime());
				}
			}

			// output final session
			context.write(outkey, NullWritable.get());
		}

	}

	public static class UserPartitioner extends
			Partitioner<UserDateWritable, Object> {
		@Override
		public int getPartition(UserDateWritable key, Object value,
				int numPartitions) {
			return key.getUser().hashCode() % numPartitions;
		}
	}

	public static class UserDateGroupComparator extends WritableComparator {

		public UserDateGroupComparator() {
			super(UserDateWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// Compare the two user IDs together, ignoring the timestamp
			UserDateWritable k1 = (UserDateWritable) a;
			UserDateWritable k2 = (UserDateWritable) b;
			return k1.getUser().compareTo(k2.getUser());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: SecondarySort <posts> <out>");
			return 1;
		}

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		// Configure job to prepare for sampling
		Job job = Job.getInstance(getConf(), "SecondarySort");
		job.setJarByClass(SecondarySort.class);

		// Use the mapper implementation with zero reduce tasks
		job.setMapperClass(UserDateMapper.class);
		job.setReducerClass(SessionBuilderReducer.class);

		job.setOutputKeyClass(UserDateWritable.class);
		job.setOutputValueClass(IdDateWritable.class);

		TextInputFormat.setInputPaths(job, input);

		// Set the output format to a text file
		TextOutputFormat.setOutputPath(job, output);

		job.setPartitionerClass(UserPartitioner.class);
		job.setGroupingComparatorClass(UserDateGroupComparator.class);

		// Submit the job and exit
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new SecondarySort(),
				args));
	}
}
