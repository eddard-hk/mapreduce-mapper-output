package org.eddard.mapreduce.protobuf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.eddard.mapreduce.Driver;

import java.io.IOException;

public class ProtobufDriver implements Driver {

    @Override
    public void run(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.map.java.opts", "-XX:+UseG1GC");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.reduce.java.opts", "-XX:+UseG1GC");
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "1073741824");

        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf, "Protobuf_Test");

        job.setJarByClass(ProtobufDriver.class);

        //Input Output Format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //Setting Mapper, Reducer.
        job.setMapperClass(ProtobufMapper.class);
        job.setReducerClass(ProtobufReducer.class);

        //Setting number of reduce tasks.
        job.setNumReduceTasks(2);

        //Input path
        FileInputFormat.addInputPath(job, new Path(input));

        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
