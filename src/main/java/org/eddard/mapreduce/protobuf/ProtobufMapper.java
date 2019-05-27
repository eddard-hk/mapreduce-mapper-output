package org.eddard.mapreduce.protobuf;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class ProtobufMapper extends Mapper<LongWritable, Text, IntWritable, BytesWritable> {

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

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Skipping header
        if (key.get() == 0) return;

        String[] columns = value.toString().split(",");

        if (columns.length <= 1) return;

        YellowProto.Value.Builder builder = YellowProto.Value.newBuilder();

        builder.setVendorID(columns[0]);
        builder.setTpepPickupDatetime(columns[1]);
        builder.setTpepDropoffDatetime(columns[2]);
        builder.setPassengerCount(Integer.valueOf(columns[3]));
        builder.setTripDistance(Float.valueOf(columns[4]));
        builder.setPuLocationID(columns[5]);
        builder.setDoLocationID(columns[6]);
        builder.setRateCodeID(columns[7]);
        builder.setStoreAndFwdFlag(columns[8]);
        builder.setPaymentType(columns[9]);
        builder.setFareAmount(Float.valueOf(columns[10]));
        builder.setExtra(Float.valueOf(columns[11]));
        builder.setMtaTax(Float.valueOf(columns[12]));
        builder.setImprovementSurcharge(Float.valueOf(columns[13]));
        builder.setTipAmount(Float.valueOf(columns[14]));
        builder.setTollsAmount(Float.valueOf(columns[15]));
        builder.setTollsAmount(Float.valueOf(columns[16]));

        context.write(new IntWritable((int) (key.get() %2)), new BytesWritable(builder.build().toByteArray()));
    }
}
