import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostCommonAlcohol {

    /*
     * Mapper:
     *
     * Đọc từng dòng CSV và xuất:
     *
     * alcoholpercentage -> 1
     *
     * Ví dụ:
     * 6.0 -> 1
     * 5.6 -> 1
     * 6.0 -> 1
     */
    public static class AlcoholMapper extends
        Mapper<LongWritable, Text, DoubleWritable, IntWritable> {

        private final DoubleWritable outputKey =
            new DoubleWritable();

        private final IntWritable outputValue =
            new IntWritable();

        private final Map<Double, Integer> localCounts =
            new HashMap<>();

        @Override
        protected void map(
            LongWritable key,
            Text value,
            Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().trim();

            // Bỏ qua dòng trống
            if (line.isEmpty()) {
                return;
            }

            int firstComma = line.indexOf(',');
            int secondComma =
                firstComma < 0
                ? -1
                : line.indexOf(',', firstComma + 1);
            int thirdComma =
                secondComma < 0
                ? -1
                : line.indexOf(',', secondComma + 1);
            int fourthComma =
                thirdComma < 0
                ? -1
                : line.indexOf(',', thirdComma + 1);

            if (fourthComma < 0) {
                context.getCounter(
                    "BEER_DATA",
                    "INVALID_COLUMN_COUNT"
                ).increment(1);

                return;
            }

            String id = line.substring(0, firstComma).trim();

            // Bỏ qua dòng tiêu đề
            if ("id".equalsIgnoreCase(id)) {
                return;
            }

            /*
             * trim() giúp xử lý lỗi khoảng trắng:
             *
             * "6"
             * " 6"
             * "6 "
             *
             * đều trở thành "6".
             */
            String alcoholText = line.substring(
                thirdComma + 1,
                fourthComma
            ).trim();

            if (alcoholText.isEmpty()) {
                context.getCounter(
                    "BEER_DATA",
                    "EMPTY_ALCOHOL"
                ).increment(1);

                return;
            }

            try {
                double alcohol =
                    Double.parseDouble(alcoholText);

                localCounts.merge(alcohol, 1, Integer::sum);

            } catch (NumberFormatException exception) {
                context.getCounter(
                    "BEER_DATA",
                    "INVALID_ALCOHOL"
                ).increment(1);
            }
        }

        @Override
        protected void cleanup(
            Context context
        ) throws IOException, InterruptedException {

            for (Map.Entry<Double, Integer> entry :
                localCounts.entrySet()) {

                outputKey.set(entry.getKey());
                outputValue.set(entry.getValue());

                context.write(outputKey, outputValue);
            }
        }
    }

    /*
     * Combiner:
     *
     * Cộng trước số lượng tại từng Mapper để giảm dữ liệu
     * truyền qua mạng.
     *
     * Ví dụ:
     * 6.0 -> [1, 1, 1]
     *
     * thành:
     * 6.0 -> 3
     */
    public static class AlcoholCombiner extends
        Reducer<DoubleWritable, IntWritable,
                DoubleWritable, IntWritable> {

        private final IntWritable outputValue =
            new IntWritable();

        @Override
        protected void reduce(
            DoubleWritable alcohol,
            Iterable<IntWritable> values,
            Context context
        ) throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            outputValue.set(total);

            context.write(alcohol, outputValue);
        }
    }

    /*
     * Reducer:
     *
     * Vì chỉ sử dụng một Reducer nên Reducer này nhận được
     * tất cả các nồng độ cồn.
     *
     * Trong reduce():
        * - Tính tổng số bia của từng nồng độ.
        * - Xuất luôn kết quả COUNT (tương đương Job 1).
        * - Ghi nhớ số lượng lớn nhất.
     *
     * Trong cleanup():
        * - Xuất kết quả MAX (tương đương Job 2).
     */
    public static class MaximumReducer extends
        Reducer<DoubleWritable, IntWritable,
                Text, IntWritable> {

        private int maximumCount = -1;

        private final List<Double> mostCommonPercentages =
            new ArrayList<>();

        private final Text outputKey = new Text();

        private final IntWritable outputValue =
            new IntWritable();

        @Override
        protected void reduce(
            DoubleWritable alcohol,
            Iterable<IntWritable> values,
            Context context
        ) throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            double percentage = alcohol.get();

            outputKey.set(
                "COUNT\t" + formatPercentage(percentage)
            );

            outputValue.set(total);

            context.write(outputKey, outputValue);

            /*
             * Nếu số lượng hiện tại lớn hơn giá trị lớn nhất:
             * - cập nhật maximumCount
             * - xóa kết quả cũ
             * - lưu nồng độ hiện tại
             */
            if (total > maximumCount) {
                maximumCount = total;

                mostCommonPercentages.clear();
                mostCommonPercentages.add(percentage);

            /*
             * Nếu bằng nhau thì lưu thêm.
             *
             * Điều này hỗ trợ trường hợp nhiều nồng độ
             * cùng phổ biến nhất.
             */
            } else if (total == maximumCount) {
                mostCommonPercentages.add(percentage);
            }
        }

        @Override
        protected void cleanup(
            Context context
        ) throws IOException, InterruptedException {

            outputValue.set(maximumCount);

            for (Double percentage :
                mostCommonPercentages) {

                outputKey.set(
                    "MAX\t" + formatPercentage(percentage)
                );

                context.write(
                    outputKey,
                    outputValue
                );
            }
        }

        private String formatPercentage(
            double percentage
        ) {
            if (percentage == Math.rint(percentage)) {
                return String.valueOf(
                    (long) percentage
                ) + "%";
            }

            return String.valueOf(percentage) + "%";
        }
    }

    public static void main(String[] args)
        throws Exception {

        if (args.length != 1) {
            System.err.println(
                "Usage: hadoop jar most-common-alcohol.jar "
                + "MostCommonAlcohol <input>"
            );

            System.err.println(
                "Example: hadoop jar most-common-alcohol.jar "
                + "MostCommonAlcohol "
                + "/input/input1/data.csv"
            );

            System.exit(1);
        }

        Configuration configuration =
            new Configuration();

        // Giảm dữ liệu shuffle để giảm thời gian truyền mạng.
        configuration.setBoolean(
            "mapreduce.map.output.compress",
            true
        );

        configuration.set(
            "mapreduce.map.output.compress.codec",
            "org.apache.hadoop.io.compress.DefaultCodec"
        );

        configuration.setFloat(
            "mapreduce.job.reduce.slowstart.completedmaps",
            0.9f
        );

        // Bật Uber task cho job nhỏ để hạn chế phải chờ cấp reducer container.
        configuration.setBoolean(
            "mapreduce.job.ubertask.enable",
            true
        );

        configuration.setInt(
            "mapreduce.job.ubertask.maxmaps",
            9
        );

        configuration.setInt(
            "mapreduce.job.ubertask.maxreduces",
            1
        );

        // Đặt default memory thấp để dễ schedule; vẫn cho phép override bằng -D.
        if (configuration.get("mapreduce.map.memory.mb") == null) {
            configuration.setInt(
                "mapreduce.map.memory.mb",
                512
            );
        }

        if (configuration.get("mapreduce.map.java.opts") == null) {
            configuration.set(
                "mapreduce.map.java.opts",
                "-Xmx384m"
            );
        }

        if (configuration.get("mapreduce.reduce.memory.mb") == null) {
            configuration.setInt(
                "mapreduce.reduce.memory.mb",
                512
            );
        }

        if (configuration.get("mapreduce.reduce.java.opts") == null) {
            configuration.set(
                "mapreduce.reduce.java.opts",
                "-Xmx384m"
            );
        }

        Job job = Job.getInstance(
            configuration,
            "Question 2 - Most common alcohol percentage"
        );

        Path outputPath = new Path("nhom5-gop");

        FileSystem fileSystem =
            FileSystem.get(configuration);

        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        job.setJarByClass(
            MostCommonAlcohol.class
        );

        job.setMapperClass(
            AlcoholMapper.class
        );

        job.setCombinerClass(
            AlcoholCombiner.class
        );

        job.setReducerClass(
            MaximumReducer.class
        );

        /*
         * Bắt buộc chỉ dùng một Reducer.
         *
         * Nếu có nhiều Reducer, mỗi Reducer chỉ tìm được
         * giá trị lớn nhất trong một phần dữ liệu.
         */
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(
            DoubleWritable.class
        );

        job.setMapOutputValueClass(
            IntWritable.class
        );

        job.setOutputKeyClass(
            Text.class
        );

        job.setOutputValueClass(
            IntWritable.class
        );

        FileInputFormat.addInputPath(
            job,
            new Path(args[0].trim())
        );

        FileOutputFormat.setOutputPath(
            job,
            outputPath
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}
