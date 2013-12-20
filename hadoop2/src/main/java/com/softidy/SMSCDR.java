package com.softidy;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class SMSCDR extends Configured implements Tool {

//    static public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//
//        private Text status = new Text();
//        private final static IntWritable addOne = new IntWritable(1);
//
//        /**
//         * Returns the SMS status code and its count
//         */
//        protected void map(LongWritable key, Text value, Context context)
//                throws java.io.IOException, InterruptedException {
//
//            //655209;1;796764372490213;804422938115889;6 is the Sample record format
//            String[] line = value.toString().split(";");
//            // If record is of SMS CDR
//            if (Integer.parseInt(line[1]) == 1) {
//                status.set(line[4]);
//                context.write(status, addOne);
//            }
//        }
//    }

    static public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text status = new Text();
        private final static IntWritable addOne = new IntWritable(1);

        static enum CDRCounter {
            NonSMSCDR;
        };

        /**
         * Returns the SMS status code and its count
         */
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {

            String[] line = value.toString().split(";");
            // If record is of SMS CDR
            if (Integer.parseInt(line[1]) == 1) {
                status.set(line[4]);
                context.write(status, addOne);
            } else {// CDR record is not of type SMS so increment the counter
                context.getCounter(CDRCounter.NonSMSCDR).increment(1);
            }
        }
    }

    static public class SMSCDRReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }
}
