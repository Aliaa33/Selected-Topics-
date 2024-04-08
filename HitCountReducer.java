
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HitCountReducer extends Reducer<Text, LongWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            sum++;
        }
        context.write(key, new Text(String.valueOf(sum)));
    }
}
