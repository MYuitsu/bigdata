import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostCommonAlcoholJob2Only {

    /*
     * Mapper cho Job 2:
     * đọc từng dòng dạng "percentage count" từ output job 1
     * và gom về cùng một key NullWritable.
     */
    public static class MaximumMapper extends
        Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void map(
            LongWritable key,
            Text value,
            Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            // Chấp nhận tab hoặc khoảng trắng.
            String[] columns = line.split("\\s+");

            if (columns.length < 2) {
                return;
            }

            String percentage = columns[0].trim();
            String count = columns[1].trim();

            context.write(
                NullWritable.get(),
                new Text(percentage + "\t" + count)
            );
        }
    }

    /*
     * Reducer cho Job 2:
     * tìm nồng độ cồn có số lượng lớn nhất.
     */
    public static class MaximumReducer extends
        Reducer<NullWritable, Text, Text, IntWritable> {

        private final Text outputKey = new Text();
        private final IntWritable outputValue =
            new IntWritable();

        @Override
        protected void reduce(
            NullWritable key,
            Iterable<Text> values,
            Context context
        ) throws IOException, InterruptedException {

            int maximumCount = -1;
            List<String> mostCommonPercentages =
                new ArrayList<>();

            for (Text value : values) {

                String line = value.toString().trim();
                String[] columns = line.split("\\t");

                if (columns.length < 2) {
                    continue;
                }

                String percentage = columns[0].trim();

                int count;

                try {
                    count = Integer.parseInt(
                        columns[1].trim()
                    );
                } catch (NumberFormatException exception) {
                    continue;
                }

                if (count > maximumCount) {
                    maximumCount = count;
                    mostCommonPercentages.clear();
                    mostCommonPercentages.add(percentage);
                } else if (count == maximumCount) {
                    mostCommonPercentages.add(percentage);
                }
            }

            for (String percentage :
                mostCommonPercentages) {

                outputKey.set(percentage);
                outputValue.set(maximumCount);

                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args)
        throws Exception {

        if (args.length != 1) {
            System.err.println(
                "Usage: hadoop jar most-common-alcohol.jar "
                + "MostCommonAlcoholJob2Only <output>"
            );
            System.err.println(
                "Example: hadoop jar most-common-alcohol.jar "
                + "MostCommonAlcoholJob2Only output/question2"
            );
            System.exit(1);
        }

        Configuration configuration =
            new Configuration();

        // Chỉ đọc đúng file part-r-00000 của output job 1.
        Path inputPath = new Path(
            "output/question2-temp/part-r-00000"
        );

        Path outputPath = new Path(args[0].trim());

        FileSystem fileSystem =
            FileSystem.get(configuration);

        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        Job maximumJob = Job.getInstance(
            configuration,
            "Question 2 - Job 2 only"
        );

        maximumJob.setJarByClass(
            MostCommonAlcoholJob2Only.class
        );

        maximumJob.setMapperClass(
            MaximumMapper.class
        );

        maximumJob.setReducerClass(
            MaximumReducer.class
        );

        // Cần 1 reducer để lấy max toàn cục.
        maximumJob.setNumReduceTasks(1);

        maximumJob.setMapOutputKeyClass(
            NullWritable.class
        );

        maximumJob.setMapOutputValueClass(
            Text.class
        );

        maximumJob.setOutputKeyClass(
            Text.class
        );

        maximumJob.setOutputValueClass(
            IntWritable.class
        );

        FileInputFormat.addInputPath(
            maximumJob,
            inputPath
        );

        FileOutputFormat.setOutputPath(
            maximumJob,
            outputPath
        );

        System.exit(
            maximumJob.waitForCompletion(true)
            ? 0
            : 1
        );
    }
}
