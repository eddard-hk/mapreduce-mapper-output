package org.eddard.mapreduce.comparable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YellowComparable implements WritableComparable<YellowComparable> {

    private String vendorID; //1
    private String tpepPickupDatetime; //2018-12-01 00:28:22
    private String tpepDropoffDatetime; //2018-12-01 00:44:07
    private Integer passengerCount; //2
    private Float tripDistance; //2.50
    private String puLocationID; //1
    private String doLocationID; //N
    private String rateCodeID; //148
    private String storeAndFwdFlag; //234
    private String paymentType; //1
    private Float fareAmount; //12
    private Float extra; //0.5
    private Float mtaTax; //0.5
    private Float improvementSurcharge; //3.95
    private Float tipAmount; //0
    private Float tollsAmount; //0.3
    private Float totalAmount; //17.25

    @Override
    public int compareTo(YellowComparable o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, vendorID);
        WritableUtils.writeString(dataOutput, tpepPickupDatetime);
        WritableUtils.writeString(dataOutput, tpepDropoffDatetime);
        dataOutput.writeInt(passengerCount);
        dataOutput.writeFloat(tripDistance);
        WritableUtils.writeString(dataOutput, puLocationID);
        WritableUtils.writeString(dataOutput, doLocationID);
        WritableUtils.writeString(dataOutput, rateCodeID);
        WritableUtils.writeString(dataOutput, storeAndFwdFlag);
        WritableUtils.writeString(dataOutput, paymentType);
        dataOutput.writeFloat(fareAmount);
        dataOutput.writeFloat(extra);
        dataOutput.writeFloat(mtaTax);
        dataOutput.writeFloat(improvementSurcharge);
        dataOutput.writeFloat(tipAmount);
        dataOutput.writeFloat(tollsAmount);
        dataOutput.writeFloat(totalAmount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        vendorID = WritableUtils.readString(dataInput);
        tpepPickupDatetime = WritableUtils.readString(dataInput);
        tpepDropoffDatetime = WritableUtils.readString(dataInput);
        passengerCount = dataInput.readInt();
        tripDistance = dataInput.readFloat();
        puLocationID = WritableUtils.readString(dataInput);
        doLocationID = WritableUtils.readString(dataInput);
        rateCodeID = WritableUtils.readString(dataInput);
        storeAndFwdFlag = WritableUtils.readString(dataInput);
        paymentType = WritableUtils.readString(dataInput);
        fareAmount = dataInput.readFloat();
        extra = dataInput.readFloat();
        mtaTax = dataInput.readFloat();
        improvementSurcharge = dataInput.readFloat();
        tipAmount = dataInput.readFloat();
        tollsAmount = dataInput.readFloat();
        totalAmount = dataInput.readFloat();
    }

    public String getVendorID() {
        return vendorID;
    }

    public void setVendorID(String vendorID) {
        this.vendorID = vendorID;
    }

    public String getTpepPickupDatetime() {
        return tpepPickupDatetime;
    }

    public void setTpepPickupDatetime(String tpepPickupDatetime) {
        this.tpepPickupDatetime = tpepPickupDatetime;
    }

    public String getTpepDropoffDatetime() {
        return tpepDropoffDatetime;
    }

    public void setTpepDropoffDatetime(String tpepDropoffDatetime) {
        this.tpepDropoffDatetime = tpepDropoffDatetime;
    }

    public Integer getPassengerCount() {
        return passengerCount;
    }

    public void setPassengerCount(Integer passengerCount) {
        this.passengerCount = passengerCount;
    }

    public Float getTripDistance() {
        return tripDistance;
    }

    public void setTripDistance(Float tripDistance) {
        this.tripDistance = tripDistance;
    }

    public String getPuLocationID() {
        return puLocationID;
    }

    public void setPuLocationID(String puLocationID) {
        this.puLocationID = puLocationID;
    }

    public String getDoLocationID() {
        return doLocationID;
    }

    public void setDoLocationID(String doLocationID) {
        this.doLocationID = doLocationID;
    }

    public String getRateCodeID() {
        return rateCodeID;
    }

    public void setRateCodeID(String rateCodeID) {
        this.rateCodeID = rateCodeID;
    }

    public String getStoreAndFwdFlag() {
        return storeAndFwdFlag;
    }

    public void setStoreAndFwdFlag(String storeAndFwdFlag) {
        this.storeAndFwdFlag = storeAndFwdFlag;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }

    public Float getFareAmount() {
        return fareAmount;
    }

    public void setFareAmount(Float fareAmount) {
        this.fareAmount = fareAmount;
    }

    public Float getExtra() {
        return extra;
    }

    public void setExtra(Float extra) {
        this.extra = extra;
    }

    public Float getMtaTax() {
        return mtaTax;
    }

    public void setMtaTax(Float mtaTax) {
        this.mtaTax = mtaTax;
    }

    public Float getImprovementSurcharge() {
        return improvementSurcharge;
    }

    public void setImprovementSurcharge(Float improvementSurcharge) {
        this.improvementSurcharge = improvementSurcharge;
    }

    public Float getTipAmount() {
        return tipAmount;
    }

    public void setTipAmount(Float tipAmount) {
        this.tipAmount = tipAmount;
    }

    public Float getTollsAmount() {
        return tollsAmount;
    }

    public void setTollsAmount(Float tollsAmount) {
        this.tollsAmount = tollsAmount;
    }

    public Float getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Float totalAmount) {
        this.totalAmount = totalAmount;
    }
}