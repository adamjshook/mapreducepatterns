package com.oreilly.mrdp.ch5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.oreilly.mrdp.ch5.ExternalJoin.ExternalJoinMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class ExternalJoinTest {

  private static final String USER_REP_KEY = "user_rep_test";
  private MapDriver<Object, Text, Text, Text> driver = null;
  private Jedis jedis = new Jedis("localhost", 6379);

  @Before
  public void setup() throws Exception {
    driver = new MapDriver<Object, Text, Text, Text>(new ExternalJoinMapper());
    driver.getConfiguration().set(ExternalJoinMapper.REDIS_HOST, "localhost");
    driver.getConfiguration().set(ExternalJoinMapper.REDIS_PORT, "6379");
    driver.getConfiguration().set(ExternalJoinMapper.REDIS_USER_REPUTATION_KEY,
        USER_REP_KEY);
    jedis.connect();
  }

  @After
  public void cleanup() {
    jedis.del(USER_REP_KEY);
  }

  @Test
  public void testInnerJoinAllData() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    // @formatter:on

    runTest("inner", inputData, userRep, output);
  }

  @Test
  public void testInnerJoinSomeBoth() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    //userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    //userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    */
    // @formatter:on

    runTest("inner", inputData, userRep, output);
  }

  @Test
  public void testInnerJoinSomeB() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    */
    // @formatter:on

    runTest("inner", inputData, userRep, output);
  }

  @Test
  public void testInnerJoinSomeA() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    //userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    */
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    // @formatter:on

    runTest("inner", inputData, userRep, output);
  }

  @Test
  public void testInnerJoinNoA() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    userRep.add(new Pair<String, String>("foo", "0"));
    //userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    //userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    //userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
    
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    */
    // @formatter:on

    runTest("inner", inputData, userRep, output);
  }

  @Test
  public void testInnerJoinNoB() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();      
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    */
    // @formatter:on

    runTest("inner", inputData, userRep, output);
  }

  @Test
  public void testLeftOuterJoinAllData() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("<row Id=\"tom\" Reputation=\"3\" />")));    
    // @formatter:on

    runTest("leftouter", inputData, userRep, output);
  }

  @Test
  public void testLeftOuterJoinSomeBoth() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    //userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    //userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    */
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("")));
    // @formatter:on

    runTest("leftouter", inputData, userRep, output);
  }

  @Test
  public void testLeftOuterJoinSomeB() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    /*output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    */
    // @formatter:on

    runTest("leftouter", inputData, userRep, output);
  }

  @Test
  public void testLeftOuterJoinSomeA() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    //userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    // @formatter:on

    runTest("leftouter", inputData, userRep, output);
  }

  @Test
  public void testLeftOuterJoinNoA() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    userRep.add(new Pair<String, String>("foo", "0"));
    //userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    //userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    //userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
    
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("")));
    // @formatter:on

    runTest("leftouter", inputData, userRep, output);
  }

  @Test
  public void testLeftOuterJoinNoB() throws Exception {
    // @formatter:off
    List<Pair<String, String>> userRep = new ArrayList<Pair<String, String>>();
    userRep.add(new Pair<String, String>("adam", "<row Id=\"adam\" Reputation=\"10\" />"));
    userRep.add(new Pair<String, String>("don", "<row Id=\"don\" Reputation=\"20000\" />"));
    userRep.add(new Pair<String, String>("tom", "<row Id=\"tom\" Reputation=\"3\" />"));
        
    List<Pair<Object, Text>> inputData = new ArrayList<Pair<Object, Text>>();      
    inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //inputData.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />"),
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />"), 
        new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />"), 
        new Text("")));
    */
    // @formatter:on

    runTest("leftouter", inputData, userRep, output);
  }

  public void runTest(String joinType, List<Pair<Object, Text>> inputData,
      List<Pair<String, String>> userRep, List<Pair<Text, Text>> output)
      throws IOException {

    for (Pair<String, String> ur : userRep) {
      jedis.hset(USER_REP_KEY, ur.getFirst(), ur.getSecond());
    }

    driver.getConfiguration().set("join.type", joinType);

    driver.withAll(inputData).withAllOutput(output);

    driver.runTest(false);
  }
}
