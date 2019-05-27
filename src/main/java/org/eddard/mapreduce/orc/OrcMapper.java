package org.eddard.mapreduce.orc;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;
import java.util.Random;

public class OrcMapper extends Mapper<LongWritable, Text, IntWritable, OrcValue> {

    //0 vendorID
    //1 tpepPickupDatetime
    //2 tpepDropoffDatetime
    //3 passengerCount
    //4 tripDistance
    //5 puLocationID
    //6 doLocationID
    //7 rateCodeID
    //8 storeAndFwdFlag
    //9 paymentType
    //10 fareAmount
    //11 extra
    //12 mtaTax
    //13 improvementSurcharge
    //14 tipAmount
    //15 tollsAmount
    //16 totalAmount
    private final Random random = new Random();

    private OrcValue orcValue = new OrcValue();
    private OrcStruct orcStruct =  (OrcStruct) OrcStruct.createValue(
            TypeDescription.fromString(OrcDriver.SCHEMA)
    );

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Skipping header
        if (key.get() == 0) return;

        String[] columns = value.toString().split(",");

        if (columns.length <= 1) return;;

        orcStruct.setFieldValue(0, new Text(columns[0]));
        orcStruct.setFieldValue(1, new Text(columns[1]));
        orcStruct.setFieldValue(2, new Text(columns[2]));
        orcStruct.setFieldValue(3, new IntWritable(Integer.parseInt(columns[3])));
        orcStruct.setFieldValue(4, new FloatWritable(Float.parseFloat(columns[4])));
        orcStruct.setFieldValue(5, new Text(columns[5]));
        orcStruct.setFieldValue(6, new Text(columns[6]));
        orcStruct.setFieldValue(7, new Text(columns[7]));
        orcStruct.setFieldValue(8, new Text(columns[8]));
        orcStruct.setFieldValue(9, new Text(columns[9]));
        orcStruct.setFieldValue(10, new FloatWritable(Float.parseFloat(columns[10])));
        orcStruct.setFieldValue(11, new FloatWritable(Float.parseFloat(columns[11])));
        orcStruct.setFieldValue(12, new FloatWritable(Float.parseFloat(columns[12])));
        orcStruct.setFieldValue(13, new FloatWritable(Float.parseFloat(columns[13])));
        orcStruct.setFieldValue(14, new FloatWritable(Float.parseFloat(columns[14])));
        orcStruct.setFieldValue(15, new FloatWritable(Float.parseFloat(columns[15])));
        orcStruct.setFieldValue(16, new FloatWritable(Float.parseFloat(columns[16])));

        orcValue.value = orcStruct;

        context.write(new IntWritable((int) (key.get() %2)),orcValue);
    }
}
