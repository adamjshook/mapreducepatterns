package com.oreilly.mrdp.ch5;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import redis.clients.jedis.Jedis;

import com.oreilly.mrdp.utils.MRDPUtils;

public class ExternalJoin extends Configured implements Tool {

  public static class ExternalJoinMapper extends
      Mapper<Object, Text, Text, Text> {

    public static final String REDIS_HOST = "mapreduce.map.externaljoinmapper.redis.host";
    public static final String REDIS_PORT = "mapreduce.map.externaljoinmapper.redis.port";
    public static final String REDIS_USER_REPUTATION_KEY = "mapreduce.map.externaljoinmapper.redis.user.rep.key";

    public enum JoinType {
      INNER, LEFTOUTER;

      public static JoinType fromString(String type) {
        if (type == null) {
          throw new InvalidParameterException(
              "Join type not set to inner, leftouter, rightouter, fullouter, or anti");
        } else if (type.equalsIgnoreCase("inner")) {
          return INNER;
        } else if (type.equalsIgnoreCase("leftouter")) {
          return LEFTOUTER;
        } else {
          throw new InvalidParameterException(
              "Join type not set to inner, leftouter, rightouter, fullouter, or anti");
        }
      }
    }

    private Text outvalue = new Text();
    private JoinType joinType = null;
    private Jedis jedis = null;
    private String userRepKey = null;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      // Get the type of join from our configuration
      joinType = JoinType.fromString(context.getConfiguration()
          .get("join.type"));

      jedis = new Jedis(getRedisHost(context.getConfiguration()),
          getRedisPort(context.getConfiguration()));

      userRepKey = getRedisUserReputationKey(context.getConfiguration());
    }

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

      String userInformation = jedis.hget(userRepKey, userId);

      // If the user information is not null, then output
      if (userInformation != null) {
        outvalue.set(userInformation);
        context.write(value, outvalue);
      } else if (joinType == JoinType.LEFTOUTER) {
        // If we are doing a left outer join, output the record with an
        // empty value
        context.write(value, new Text(""));
      }
    }

    public static void setRedisInfo(Job job, String host, int port) {
      job.getConfiguration().set(REDIS_HOST, host);
      job.getConfiguration().setInt(REDIS_PORT, port);
    }

    public static String getRedisHost(Configuration conf) {
      return conf.get(REDIS_HOST, "localhost");
    }

    public static int getRedisPort(Configuration conf) {
      return conf.getInt(REDIS_PORT, 6379);
    }

    public static void setRedisUserReputationKey(Job job, String key) {
      job.getConfiguration().set(REDIS_USER_REPUTATION_KEY, key);
    }

    public static String getRedisUserReputationKey(Configuration conf) {
      if (conf.get(REDIS_USER_REPUTATION_KEY) == null) {
        throw new InvalidParameterException(REDIS_USER_REPUTATION_KEY
            + " is not set in configuration");
      }

      return conf.get(REDIS_USER_REPUTATION_KEY);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 5) {
      System.err
          .println("Usage: ExternalJoin <user data> <redis_host> <redis_port> <out> [inner|leftouter]");
      System.exit(1);
    }

    Path userInput = new Path(args[0]);
    String redisHost = args[1];
    int redisPort = Integer.parseInt(args[2]);
    Path outputDir = new Path(args[3]);
    String joinType = args[4];
    // validate the join type. throws an exception if not a valid string
    ExternalJoinMapper.JoinType.fromString(joinType);

    // Configure the join type
    Job job = Job.getInstance(getConf(), "External Join");
    job.getConfiguration().set("join.type", joinType);
    job.setJarByClass(ExternalJoin.class);

    job.setMapperClass(ExternalJoinMapper.class);

    // Configure the Redis information
    ExternalJoinMapper.setRedisInfo(job, redisHost, redisPort);
    ExternalJoinMapper.setRedisUserReputationKey(job, "user_repo");

    job.setNumReduceTasks(0);

    TextInputFormat.setInputPaths(job, userInput);
    TextOutputFormat.setOutputPath(job, outputDir);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new ExternalJoin(), args);
  }
}
