package org.eddard.mapreduce.orc;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OrcReducer extends Reducer<IntWritable, OrcValue, NullWritable, Text> {

    private final NullWritable nullWritable = NullWritable.get();

    @Override
    public void reduce(IntWritable key, Iterable<OrcValue> values, Context context) throws IOException, InterruptedException {

        List<String> outputList;

        for (OrcValue orcValue : values) {
            outputList = new ArrayList<>();
            OrcStruct orcStruct = (OrcStruct) orcValue.value;

            outputList.add(((Text) orcStruct.getFieldValue(0)).toString());
            outputList.add(((Text) orcStruct.getFieldValue(1)).toString());
            outputList.add(((Text) orcStruct.getFieldValue(2)).toString());
            outputList.add(String.valueOf(((IntWritable) orcStruct.getFieldValue(3)).get()));
            outputList.add(String.valueOf(((FloatWritable) orcStruct.getFieldValue(4)).get()));
            outputList.add(((Text) orcStruct.getFieldValue(5)).toString());
            outputList.add(((Text) orcStruct.getFieldValue(6)).toString());
            outputList.add(((Text) orcStruct.getFieldValue(7)).toString());
            outputList.add(((Text) orcStruct.getFieldValue(8)).toString());
            outputList.add(((Text) orcStruct.getFieldValue(9)).toString());
            outputList.add(String.valueOf(((FloatWritable) orcStruct.getFieldValue(10)).get()));
            outputList.add(String.valueOf(((FloatWritable) orcStruct.getFieldValue(11)).get()));
            outputList.add(String.valueOf(((FloatWritable) orcStruct.getFieldValue(12)).get()));
            outputList.add(String.valueOf(((FloatWritable) orcStruct.getFieldValue(13)).get()));
            outputList.add(String.valueOf(((FloatWritable) orcStruct.getFieldValue(14)).get()));
            outputList.add(String.valueOf(((FloatWritable) orcStruct.getFieldValue(15)).get()));
            outputList.add(String.valueOf(((FloatWritable) orcStruct.getFieldValue(16)).get()));

            //context.write(nullWritable, new Text(String.join(",", outputList)));
        }
    }
}
