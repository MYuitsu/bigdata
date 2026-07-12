import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        private static final IntWritable ONE = new IntWritable(1);

        private final DoubleWritable outputKey =
            new DoubleWritable();

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

            /*
             * Tách tối đa thành 5 cột:
             *
             * 0: id
             * 1: make
             * 2: type
             * 3: alcoholpercentage
             * 4: brewery
             */
            String[] columns = line.split(",", 5);

            if (columns.length < 5) {
                context.getCounter(
                    "BEER_DATA",
                    "INVALID_COLUMN_COUNT"
                ).increment(1);

                return;
            }

            String id = columns[0].trim();

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
            String alcoholText = columns[3].trim();

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

                outputKey.set(alcohol);

                context.write(outputKey, ONE);

            } catch (NumberFormatException exception) {
                context.getCounter(
                    "BEER_DATA",
                    "INVALID_ALCOHOL"
                ).increment(1);
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
     * - Ghi nhớ số lượng lớn nhất.
     *
     * Trong cleanup():
     * - Xuất nồng độ phổ biến nhất.
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
                    formatPercentage(percentage)
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
