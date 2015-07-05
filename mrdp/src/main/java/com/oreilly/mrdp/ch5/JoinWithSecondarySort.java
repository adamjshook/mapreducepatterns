package com.oreilly.mrdp.ch5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oreilly.mrdp.utils.MRDPUtils;

public class JoinWithSecondarySort extends Configured implements Tool {

  public static class TextPair implements WritableComparable<TextPair> {

    private Text first = new Text();
    private Text second = new Text();

    public void setFirst(Text first) {
      this.first.set(first);
    }

    public Text getFirst() {
      return this.first;
    }

    public void setSecond(Text second) {
      this.second.set(second);
    }

    public Text getSecond() {
      return this.second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.first.write(out);
      this.second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.first.readFields(in);
      this.second.readFields(in);

    }

    @Override
    public int compareTo(TextPair o) {
      int code = this.first.compareTo(o.getFirst());
      if (code == 0) {
        return this.second.compareTo(o.getSecond());
      }
      return code;
    }
  }

  public static class TextPairPartitioner extends Partitioner<TextPair, Object> {
    @Override
    public int getPartition(TextPair key, Object value, int numPartitions) {
      return key.getFirst().hashCode() % numPartitions;
    }
  }

  public static class TextPairGroupComparator extends WritableComparator {

    public TextPairGroupComparator() {
      super(TextPair.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      // Compare the two user IDs together, ignoring the timestamp
      TextPair k1 = (TextPair) a;
      TextPair k2 = (TextPair) b;
      return k1.getFirst().compareTo(k2.getFirst());
    }
  }

  public static class UserJoinMapper extends
      Mapper<Object, Text, TextPair, Text> {

    private TextPair outkey = new TextPair();
    private Text outvalue = new Text();

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      // Parse the input string into a nice map
      Map<String, String> parsed = MRDPUtils
          .transformXmlToMap(value.toString());

      String userId = parsed.get("Id");
      if (userId == null) {
        return;
      }

      // The foreign join key is the user ID
      outkey.getFirst().set(userId);

      // Set the second element of our pair as the 'A' data set
      outkey.getSecond().set("A");

      // Flag this record for the reducer and then output
      outvalue.set(value);

      context.write(outkey, outvalue);
    }
  }

  public static class CommentJoinMapper extends
      Mapper<Object, Text, TextPair, Text> {

    private TextPair outkey = new TextPair();
    private Text outvalue = new Text();

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      // Parse the input string into a nice map
      Map<String, String> parsed = MRDPUtils
          .transformXmlToMap(value.toString());

      String userId = parsed.get("UserId");
      if (userId == null) {
        return;
      }

      // The foreign join key is the user ID
      outkey.getFirst().set(userId);

      // Set the second element of our pair as the 'B' data set
      outkey.getSecond().set("B");

      // Flag this record for the reducer and then output
      outvalue.set(value);
      context.write(outkey, outvalue);
    }
  }

  public static class UserJoinReducer extends
      Reducer<TextPair, Text, Text, Text> {

    private List<Text> dataFromA = new ArrayList<Text>();
    private JoinType joinType = null;
    private static final Text EMPTY_TEXT = new Text("");

    private enum JoinType {
      INNER, LEFTOUTER, RIGHTOUTER, FULLOUTER, ANTI;

      public static JoinType fromString(String type) {
        if (type == null) {
          throw new InvalidParameterException(
              "Join type not set to inner, leftouter, rightouter, fullouter, or anti");
        } else if (type.equalsIgnoreCase("inner")) {
          return INNER;
        } else if (type.equalsIgnoreCase("leftouter")) {
          return LEFTOUTER;
        } else if (type.equalsIgnoreCase("rightouter")) {
          return RIGHTOUTER;
        } else if (type.equalsIgnoreCase("fullouter")) {
          return FULLOUTER;
        } else if (type.equalsIgnoreCase("anti")) {
          return ANTI;
        } else {
          throw new InvalidParameterException(
              "Join type not set to inner, leftouter, rightouter, fullouter, or anti");
        }
      }
    }

    @Override
    public void setup(Context context) {
      // Get the type of join from our configuration
      joinType = JoinType.fromString(context.getConfiguration()
          .get("join.type"));
    }

    @Override
    public void reduce(TextPair key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      // Clear our list
      dataFromA.clear();

      boolean hasBData = false;
      // iterate through all our values, checking the second of our key pair for
      // when data from 'A' switches to 'B'
      for (Text t : values) {

        if (key.getSecond().toString().equals("A")) {
          // add these values to our list of records from A
          dataFromA.add(new Text(t));
        } else {
          // we have switched over to data from B, so we can join records from B
          // onto A without pulling B into memory

          // First, set our flag that we have data from B
          hasBData = true;

          switch (joinType) {
          case INNER:
            // INNER case, if A is empty, there will be no output
            for (Text recordA : dataFromA) {
              context.write(recordA, t);
            }
            break;
          case LEFTOUTER:
            // LEFTOUTER case, is A is non-empty, output record A with B
            if (!dataFromA.isEmpty()) {
              for (Text recordA : dataFromA) {
                context.write(recordA, t);
              }
            }
            break;
          case RIGHTOUTER:
            // RIGHTOUTER case, if A is empty, output record B with empty text
            if (dataFromA.isEmpty()) {
              context.write(EMPTY_TEXT, t);
            } else {
              // if A is non-empty, output A records and B records
              for (Text recordA : dataFromA) {
                context.write(recordA, t);
              }
            }
            break;
          case FULLOUTER:
            // FULLOUTER case, if A is empty, output record B with empty text
            if (dataFromA.isEmpty()) {
              context.write(EMPTY_TEXT, t);
            } else {
              // if A is non-empty, output A records and B records
              for (Text recordA : dataFromA) {
                context.write(recordA, t);
              }
            }
            break;
          case ANTI:
            // ANTI case, only output records from B if A is empty
            if (dataFromA.isEmpty()) {
              context.write(EMPTY_TEXT, t);
            }
          }
        }
      }// end record iteration

      // If we don't have data from B, make sure to output empty text records
      // for appropriate cases
      if (!hasBData) {
        switch (joinType) {
        case LEFTOUTER:
        case FULLOUTER:
        case ANTI:
          for (Text recordA : dataFromA) {
            context.write(recordA, EMPTY_TEXT);
          }
          break;
        case INNER:
        case RIGHTOUTER:
          // noop
          break;
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new JoinWithSecondarySort(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      System.err
          .println("Usage: JoinWithSecondarySort <user data> <comment data> <out> [inner|leftouter|rightouter|fullouter|anti]");
      return 2;
    }

    Path userInput = new Path(args[0]);
    Path commentInput = new Path(args[1]);
    Path outputDir = new Path(args[2]);
    String joinType = args[3];

    // validate the join type. throws an exception if not a valid string
    UserJoinReducer.JoinType.fromString(args[3]);
    Job job = Job.getInstance(getConf(), "Join with Secondary Sort");

    // Configure the join type
    job.getConfiguration().set("join.type", joinType);
    job.setJarByClass(JoinWithSecondarySort.class);

    // Use multiple inputs to set which input uses what mapper
    // This will keep parsing of each data set separate from a logical
    // standpoint
    MultipleInputs.addInputPath(job, userInput, TextInputFormat.class,
        UserJoinMapper.class);

    MultipleInputs.addInputPath(job, commentInput, TextInputFormat.class,
        CommentJoinMapper.class);

    job.setReducerClass(UserJoinReducer.class);

    FileOutputFormat.setOutputPath(job, outputDir);

    job.setPartitionerClass(TextPairPartitioner.class);
    job.setGroupingComparatorClass(TextPairGroupComparator.class);

    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
