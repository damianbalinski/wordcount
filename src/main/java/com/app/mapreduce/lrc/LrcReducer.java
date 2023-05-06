package com.app.mapreduce.lrc;

import lombok.SneakyThrows;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class LrcReducer extends Reducer<Text, LrcMapperValue, Text, DoubleWritable> {
    public static int N = 25;

    @SneakyThrows
    public void reduce(Text key, Iterable<LrcMapperValue> initialValues, Context context) throws IOException, InterruptedException {
        List<LrcMapperValue> values = new ArrayList<>();
        for (LrcMapperValue v : initialValues) {
            values.add(v.clone()); // it's a fucking joke
        }
        values.sort(Comparator.comparingLong(LrcMapperValue::getTimestamp));

        for (int index = N - 1; index < values.size(); index++){
            LrcMapperValue value = values.get(index);
            double lrc = countLrc(values, index);

            context.write(
                    new Text(value.getSymbol() + ":" + value.getTimestamp()),
                    new DoubleWritable(lrc * 100));
        }
    }

    private double countLrc(List<LrcMapperValue> values, int index) {
        double max = Double.MIN_VALUE;
        for (int i = N - 1; i >= 0; i--) {
            max = Math.max(max, values.get(index - i).getPrice());
        }

        return Math.min(0, (values.get(index).getPrice() - max) / max);
    }
}
