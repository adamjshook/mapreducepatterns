package com.oreilly.mrdp.ch5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.oreilly.mrdp.ch5.ReduceSideJoin.CommentJoinMapper;
import com.oreilly.mrdp.ch5.ReduceSideJoin.UserJoinMapper;
import com.oreilly.mrdp.ch5.ReduceSideJoin.UserJoinReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReduceSideJoinTest {

  private MultipleInputsMapReduceDriver<Text, Text, Text, Text> driver = null;

  @Before
  public void setup() throws Exception {
    driver = new MultipleInputsMapReduceDriver<>();
  }

  @After
  public void cleanup() {
  }

  @Test
  public void testInnerJoinAllData() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("inner", dataA, dataB, output);
  }

  @Test
  public void testInnerJoinSomeBoth() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    */
    // @formatter:on

    runTest("inner", dataA, dataB, output);
  }

  @Test
  public void testInnerJoinSomeB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();    
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    */
    // @formatter:on

    runTest("inner", dataA, dataB, output);
  }

  @Test
  public void testInnerJoinSomeA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    */
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on
    // @formatter:on

    runTest("inner", dataA, dataB, output);
  }

  @Test
  public void testInnerJoinNoA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    */
    // @formatter:on

    runTest("inner", dataA, dataB, output);
  }

  @Test
  public void testInnerJoinNoB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();      
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    */
    // @formatter:on

    runTest("inner", dataA, dataB, output);
  }

  @Test
  public void testLeftOuterJoinAllData() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("leftouter", dataA, dataB, output);
  }

  @Test
  public void testLeftOuterJoinSomeBoth() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    */
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("")));
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    */
    // @formatter:on

    runTest("leftouter", dataA, dataB, output);
  }

  @Test
  public void testLeftOuterJoinSomeB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("")));
    // @formatter:on

    runTest("leftouter", dataA, dataB, output);
  }

  @Test
  public void testLeftOuterJoinSomeA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    */
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("leftouter", dataA, dataB, output);
  }

  @Test
  public void testLeftOuterJoinNoA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    */
    // @formatter:on

    runTest("leftouter", dataA, dataB, output);
  }

  @Test
  public void testLeftOuterJoinNoB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();      
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"),
        new Text("")));  
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"),
        new Text("")));  
    // @formatter:on

    runTest("leftouter", dataA, dataB, output);
  }

  @Test
  public void testRightOuterJoinAllData() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("rightouter", dataA, dataB, output);
  }

  @Test
  public void testRightOuterJoinSomeBoth() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    */
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("rightouter", dataA, dataB, output);
  }

  @Test
  public void testRightOuterJoinSomeB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();    
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    */
    // @formatter:on

    runTest("rightouter", dataA, dataB, output);
  }

  @Test
  public void testRightOuterJoinSomeA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));    
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on
    // @formatter:on

    runTest("rightouter", dataA, dataB, output);
  }

  @Test
  public void testRightOuterJoinNoA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("rightouter", dataA, dataB, output);
  }

  @Test
  public void testRightOuterJoinNoB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();      
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    */
    // @formatter:on

    runTest("rightouter", dataA, dataB, output);
  }

  @Test
  public void testFullOuterJoinAllData() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("fullouter", dataA, dataB, output);
  }

  @Test
  public void testFullOuterJoinSomeBoth() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    */
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("fullouter", dataA, dataB, output);
  }

  @Test
  public void testFullOuterJoinSomeB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();    
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("")));
    // @formatter:on

    runTest("fullouter", dataA, dataB, output);
  }

  @Test
  public void testFullOuterJoinSomeA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""),
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on
    // @formatter:on

    runTest("fullouter", dataA, dataB, output);
  }

  @Test
  public void testFullOuterJoinNoA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""),
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("fullouter", dataA, dataB, output);
  }

  @Test
  public void testFullOuterJoinNoB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();      
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("")));
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    */
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("")));
    // @formatter:on

    runTest("fullouter", dataA, dataB, output);
  }

  @Test
  public void testAntiJoinAllData() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
        */
    // @formatter:on

    runTest("anti", dataA, dataB, output);
  }

  @Test
  public void testAntiJoinSomeBoth() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    */
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text(""),
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("anti", dataA, dataB, output);
  }

  @Test
  public void testAntiJoinSomeB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("")));
    // @formatter:on

    runTest("anti", dataA, dataB, output);
  }

  @Test
  public void testAntiJoinSomeA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    /*
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    */
    // @formatter:on

    runTest("anti", dataA, dataB, output);
  }

  @Test
  public void testAntiJoinNoA() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    //dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
    
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text(""),
        new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    output.add(new Pair<Text, Text>(
        new Text(""), 
        new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    // @formatter:on

    runTest("anti", dataA, dataB, output);
  }

  @Test
  public void testAntiJoinNoB() throws Exception {
    // @formatter:off
    List<Pair<Object, Text>> dataA = new ArrayList<Pair<Object, Text>>();
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"adam\" Reputation=\"10\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"don\" Reputation=\"20000\" />")));
    dataA.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"tom\" Reputation=\"3\" />")));
        
    List<Pair<Object, Text>> dataB = new ArrayList<Pair<Object, Text>>();      
    dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row/>")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"1\" UserId=\"adam\" Comment=\"MRDPv1 Perfect Code Paradigm\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"2\" UserId=\"adam\" Comment=\"MRDPv2 TDD\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"3\" UserId=\"don\" Comment=\"mrjob here we go\" />")));
    //dataB.add(new Pair<Object, Text>(new LongWritable(0), new Text("<row Id=\"4\" UserId=\"tom\" Comment=\"fitbit\" />")));
    
    List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"adam\" Reputation=\"10\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"don\" Reputation=\"20000\" />"), 
        new Text("")));
    output.add(new Pair<Text, Text>(
        new Text("<row Id=\"tom\" Reputation=\"3\" />"), 
        new Text("")));
    // @formatter:on

    runTest("anti", dataA, dataB, output);
  }

  public void runTest(String joinType, List<Pair<Object, Text>> dataA,
      List<Pair<Object, Text>> dataB, List<Pair<Text, Text>> output)
      throws IOException {

    UserJoinMapper userMapper = new UserJoinMapper();
    CommentJoinMapper commentMapper = new CommentJoinMapper();

    driver.getConfiguration().set("join.type", joinType);

    driver.withMapper(userMapper).withMapper(commentMapper)
        .withReducer(new UserJoinReducer()).withAll(userMapper, dataA)
        .withAll(commentMapper, dataB).withAllOutput(output);

    driver.runTest(false);
  }
}
