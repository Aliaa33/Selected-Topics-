
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class HitCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text monthUserIP = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(" ");
        String user_ip = parts[0]; // Assuming the user IP is at index 0 in the log entry
        String date = parts[3].split("/")[1]; // Get date without '['
        //String month_str = date.split("/")[1];
        String outputKey = date + ", " + user_ip;
        monthUserIP.set(outputKey);
        context.write(monthUserIP, new LongWritable(1));
    }
}