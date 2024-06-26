
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class HitCountPartitioner extends Partitioner<Text, LongWritable> implements Configurable {
    private Configuration configuration;
    private HashMap<String, Integer> months = new HashMap<>();

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
        // Initialize the mapping of month names to partition numbers
        months.put("Jan", 0);
        months.put("Feb", 1);
        months.put("Mar", 2);
        months.put("Apr", 3);
        months.put("May", 4);
        months.put("Jun", 5);
        months.put("Jul", 6);
        months.put("Aug", 7);
        months.put("Sep", 8);
        months.put("Oct", 9);
        months.put("Nov", 10);
        months.put("Dec", 11);
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public int getPartition(Text key, LongWritable value, int numPartitions) {
        String[] parts = key.toString().split(", "); //month, ip
        String month = parts[0]; // Extract the month from the key
        return months.get(month);
    }
}