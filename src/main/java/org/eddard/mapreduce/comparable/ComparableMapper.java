package org.eddard.mapreduce.comparable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class ComparableMapper extends Mapper<LongWritable, Text, IntWritable, YellowComparable> {

    private Logger logger = LoggerFactory.getLogger(ComparableMapper.class);

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
    private final Random random = new Random();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Skipping header
        if (key.get() == 0) return;

        String[] columns = value.toString().split(",");

        if (columns.length <= 1) return;;

        YellowComparable yellow = new YellowComparable();

        yellow.setVendorID(columns[0]);
        yellow.setTpepPickupDatetime(columns[1]);
        yellow.setTpepDropoffDatetime(columns[2]);
        yellow.setPassengerCount(Integer.valueOf(columns[3]));
        yellow.setTripDistance(Float.valueOf(columns[4]));
        yellow.setPuLocationID(columns[5]);
        yellow.setDoLocationID(columns[6]);
        yellow.setRateCodeID(columns[7]);
        yellow.setStoreAndFwdFlag(columns[8]);
        yellow.setPaymentType(columns[9]);
        yellow.setFareAmount(Float.parseFloat(columns[10]));
        yellow.setExtra(Float.parseFloat(columns[11]));
        yellow.setMtaTax(Float.parseFloat(columns[12]));
        yellow.setImprovementSurcharge(Float.parseFloat(columns[13]));
        yellow.setTipAmount(Float.parseFloat(columns[14]));
        yellow.setTollsAmount(Float.parseFloat(columns[15]));
        yellow.setTotalAmount(Float.parseFloat(columns[16]));

        context.write(new IntWritable((int) (key.get() %2)), yellow);
    }

}
