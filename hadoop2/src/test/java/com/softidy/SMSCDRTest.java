package com.softidy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;

public class SMSCDRTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        SMSCDR.SMSCDRMapper mapper = new SMSCDR.SMSCDRMapper();
        SMSCDR.SMSCDRReducer reducer = new SMSCDR.SMSCDRReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("655209;1;796764372490213;804422938115889;6"))
                .withInput(new LongWritable(), new Text("353415;0;356857119806206;287572231184798;4"))
                .withInput(new LongWritable(), new Text("353415;0;356857119806206;287572231184798;4"))
                .withInput(new LongWritable(), new Text("835699;1;252280313968413;889717902341635;0"))
                .withOutput(new Text("6"), new IntWritable(1))
                .withOutput(new Text("0"), new IntWritable(1))
                .runTest();

        assertEquals("Expected 1 counter increment", 2, mapDriver.getCounters()
                .findCounter(SMSCDR.SMSCDRMapper.CDRCounter.NonSMSCDR).getValue());
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = asList(new IntWritable(1), new IntWritable(1));

        reduceDriver.withInput(new Text("6"), values)
                .withOutput(new Text("6"), new IntWritable(2))
                .runTest();
    }
}