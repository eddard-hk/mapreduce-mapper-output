package org.eddard.mapreduce;

import java.io.IOException;

public interface Driver {

    public void run(String input, String output) throws IOException, ClassNotFoundException, InterruptedException;
}
