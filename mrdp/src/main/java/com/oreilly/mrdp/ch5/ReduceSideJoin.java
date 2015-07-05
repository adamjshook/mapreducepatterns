package com.oreilly.mrdp.ch5;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oreilly.mrdp.utils.MRDPUtils;

public class ReduceSideJoin extends Configured implements Tool {

  public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
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
      outkey.set(userId);

      // Flag this record for the reducer and then output
      outvalue.set("A" + value.toString());
      context.write(outkey, outvalue);
    }
  }

  public static class CommentJoinMapper extends
      Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
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
      outkey.set(userId);

      // Flag this record for the reducer and then output
      outvalue.set("B" + value.toString());
      context.write(outkey, outvalue);
    }
  }

  public static class UserJoinReducer extends Reducer<Text, Text, Text, Text> {

    public enum JoinType {
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

    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private JoinType joinType = null;

    @Override
    public void setup(Context context) {
      // Get the type of join from our configuration
      joinType = JoinType.fromString(context.getConfiguration()
          .get("join.type"));
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      // Clear our lists
      listA.clear();
      listB.clear();

      // iterate through all our values, binning each record based on what
      // it was tagged with
      // make sure to remove the tag!
      for (Text t : values) {
        if (t.charAt(0) == 'A') {
          listA.add(new Text(t.toString().substring(1)));
        } else /* if (t.charAt(0) == 'B') */{
          listB.add(new Text(t.toString().substring(1)));
        }
      }

      // Execute our join logic now that the lists are filled
      executeJoinLogic(context);
    }

    private void executeJoinLogic(Context context) throws IOException,
        InterruptedException {
      switch (joinType) {
      case INNER:
        // If both lists are not empty, join A with B
        if (!listA.isEmpty() && !listB.isEmpty()) {
          for (Text A : listA) {
            for (Text B : listB) {
              context.write(A, B);
            }
          }
        }
        break;
      case LEFTOUTER:
        // For each entry in A,
        for (Text A : listA) {
          // If list B is not empty, join A and B
          if (!listB.isEmpty()) {
            for (Text B : listB) {
              context.write(A, B);
            }
          } else {
            // Else, output A by itself
            context.write(A, new Text(""));
          }
        }
        break;
      case RIGHTOUTER:
        // For each entry in B,
        for (Text B : listB) {
          // If list A is not empty, join A and B
          if (!listA.isEmpty()) {
            for (Text A : listA) {
              context.write(A, B);
            }
          } else {
            // Else, output B by itself
            context.write(new Text(""), B);
          }
        }
        break;
      case FULLOUTER:
        // If list A is not empty
        if (!listA.isEmpty()) {
          // For each entry in A
          for (Text A : listA) {
            // If list B is not empty, join A with B
            if (!listB.isEmpty()) {
              for (Text B : listB) {
                context.write(A, B);
              }
            } else {
              // Else, output A by itself
              context.write(A, new Text(""));
            }
          }
        } else {
          // If list A is empty, just output B
          for (Text B : listB) {
            context.write(new Text(""), B);
          }
        }
        break;
      case ANTI:
        // If list A is empty and B is empty or vice versa
        if (listA.isEmpty() ^ listB.isEmpty()) {

          // Iterate both A and B with null values
          // The previous XOR check will make sure exactly one of
          // these lists is empty and therefore won't have output
          for (Text A : listA) {
            context.write(A, new Text(""));
          }

          for (Text B : listB) {
            context.write(new Text(""), B);
          }
        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      System.err
          .println("Usage: ReduceSideJoin <user data> <comment data> <out> [inner|leftouter|rightouter|fullouter|anti]");
      System.exit(1);
    }

    Path userInput = new Path(args[0]);
    Path commentInput = new Path(args[1]);
    Path outputDir = new Path(args[2]);
    String joinType = args[3];
    // validate the join type. throws an exception if not a valid string
    UserJoinReducer.JoinType.fromString(joinType);

    Job job = Job.getInstance(getConf(), "Reduce Side Join");

    // Configure the join type
    job.getConfiguration().set("join.type", joinType);
    job.setJarByClass(ReduceSideJoin.class);

    // Use multiple inputs to set which input uses what mapper
    // This will keep parsing of each data set separate from a logical
    // standpoint
    MultipleInputs.addInputPath(job, userInput, TextInputFormat.class,
        UserJoinMapper.class);

    MultipleInputs.addInputPath(job, commentInput, TextInputFormat.class,
        CommentJoinMapper.class);

    job.setReducerClass(UserJoinReducer.class);

    FileOutputFormat.setOutputPath(job, outputDir);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new ReduceSideJoin(), args);
  }
}
