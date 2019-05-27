package org.eddard.mapreduce.protobuf;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProtobufReducer extends Reducer<IntWritable, BytesWritable, NullWritable, Text> {

    private final NullWritable nullWritable = NullWritable.get();

    @Override
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

        List<String> outputList;

        for (BytesWritable bytesWritable : values) {
            outputList = new ArrayList<>();
            YellowProto.Value protoValue = YellowProto.Value.parseFrom(bytesWritable.copyBytes());

            outputList.add(protoValue.getVendorID());
            outputList.add(protoValue.getTpepPickupDatetime());
            outputList.add(protoValue.getTpepDropoffDatetime());
            outputList.add(String.valueOf(protoValue.getPassengerCount()));
            outputList.add(String.valueOf(protoValue.getTripDistance()));
            outputList.add(protoValue.getPuLocationID());
            outputList.add(protoValue.getDoLocationID());
            outputList.add(protoValue.getRateCodeID());
            outputList.add(protoValue.getStoreAndFwdFlag());
            outputList.add(protoValue.getPaymentType());
            outputList.add(String.valueOf(protoValue.getFareAmount()));
            outputList.add(String.valueOf(protoValue.getExtra()));
            outputList.add(String.valueOf(protoValue.getMtaTax()));
            outputList.add(String.valueOf(protoValue.getImprovementSurcharge()));
            outputList.add(String.valueOf(protoValue.getTipAmount()));
            outputList.add(String.valueOf(protoValue.getTollsAmount()));
            outputList.add(String.valueOf(protoValue.getTollsAmount()));

            //context.write(nullWritable, new Text(String.join(",", outputList)));
        }
    }
}
