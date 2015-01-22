import java.util.ArrayList;
import java.util.List;
import java.lang.System;
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;


import org.junit.Before;
import org.junit.Test;

import nl.utwente.bigdata.TwitterExample;
 
public class TwitterExampleTest {
 
  private MapDriver<Object, Text, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;
  private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver; 
 
  @Before
  public void setUp() {
    TwitterExample.ExampleMapper mapper   = new TwitterExample.ExampleMapper();
    TwitterExample.ExampleReducer reducer = new TwitterExample.ExampleReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 

  @Test
  public void testMapper() throws IOException {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"LUIS suarez is een baas\",\"id_str\":\"1\"}");
    mapDriver.withInput(key, value);
    mapDriver.withOutput(new Text("LUIS SUAREZ"), new Text("1"));
    mapDriver.runTest();
  }
 

  @Test
  public void testReducer() throws IOException {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("1"));
    values.add(new Text("1"));
    values.add(new Text("1"));
    reduceDriver.withInput(new Text("LUIZ"), values);
    reduceDriver.withOutput(new Text("LUIZ"), new Text("3"));
    reduceDriver.runTest();
  }


  @Test
  public void testMapReduce() throws IOException  {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"LUIS suarez is een vedette\",\"id_str\":\"1\"}");
    mapReduceDriver.withInput(key, value);
    mapReduceDriver.withOutput(new Text("LUIS SUAREZ"), new Text("1"));
    mapReduceDriver.runTest();
  }

}
