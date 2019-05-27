package org.eddard.mapreduce.comparable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ComparableReducer extends Reducer<IntWritable, YellowComparable, NullWritable, Text> {

    //vendorID
    //tpepPickupDatetime
    //tpepDropoffDatetime
    //passengerCount
    //tripDistance
    //puLocationID
    //doLocationID
    //rateCodeID
    //storeAndFwdFlag
    //paymentType
    //fareAmount
    //extra
    //mtaTax
    //improvementSurcharge
    //tipAmount
    //tollsAmount
    //totalAmount

    private final NullWritable nullWritable = NullWritable.get();

    @Override
    public void reduce(IntWritable key, Iterable<YellowComparable> values, Context context) throws IOException, InterruptedException {

        List<String> outputList;

        for (YellowComparable yellow : values) {
            outputList = new ArrayList<>();

            outputList.add(yellow.getVendorID());
            outputList.add(yellow.getTpepPickupDatetime());
            outputList.add(yellow.getTpepDropoffDatetime());
            outputList.add(String.valueOf(yellow.getPassengerCount()));
            outputList.add(String.valueOf(yellow.getPassengerCount()));
            outputList.add(yellow.getPuLocationID());
            outputList.add(yellow.getDoLocationID());
            outputList.add(yellow.getRateCodeID());
            outputList.add(yellow.getStoreAndFwdFlag());
            outputList.add(yellow.getPaymentType());
            outputList.add(String.valueOf(yellow.getFareAmount()));
            outputList.add(String.valueOf(yellow.getExtra()));
            outputList.add(String.valueOf(yellow.getMtaTax()));
            outputList.add(String.valueOf(yellow.getImprovementSurcharge()));
            outputList.add(String.valueOf(yellow.getTipAmount()));
            outputList.add(String.valueOf(yellow.getTollsAmount()));
            outputList.add(String.valueOf(yellow.getTotalAmount()));

            //context.write(nullWritable, new Text(String.join(",", outputList)));
        }
    }
}
