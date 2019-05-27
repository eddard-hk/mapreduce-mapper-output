package org.eddard.mapreduce;

import org.eddard.mapreduce.comparable.ComparableDriver;
import org.eddard.mapreduce.orc.OrcDriver;
import org.eddard.mapreduce.protobuf.ProtobufDriver;

import java.io.IOException;

public class Main {

    //hadoop jar mapreduce-mapper-output-1.0-SNAPSHOT.jar org.eddard.mapreduce.Main comparable /data/nyctaxi/yellow /output
    //hadoop jar mapreduce-mapper-output-1.0-SNAPSHOT.jar org.eddard.mapreduce.Main orc /data/nyctaxi/yellow /output
    //hadoop jar mapreduce-mapper-output-1.0-SNAPSHOT.jar org.eddard.mapreduce.Main protobuf /data/nyctaxi/yellow /output

    public static void main(String args[]) throws InterruptedException, IOException, ClassNotFoundException {
        Driver driver;

        switch (args[0]) {
            case "comparable":
                driver = new ComparableDriver();
                break;
            case "orc":
                driver = new OrcDriver();
                break;
            case "protobuf":
                driver = new ProtobufDriver();
                break;
            default:
                throw new IOException("Not supported type");
        }

        driver.run(args[1], args[2]);
    }
}
