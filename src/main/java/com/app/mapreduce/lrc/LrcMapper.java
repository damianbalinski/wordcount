package com.app.mapreduce.lrc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.SneakyThrows;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class LrcMapper extends Mapper<LongWritable, Text, Text, LrcMapperValue> {
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        StockEntryDto dto = parseInput(line.toString());
        LrcMapperValue value = convertToValue(dto);
        context.write(new Text(value.getSymbol()), value);
    }

    private StockEntryDto parseInput(String input) {
        JsonObject jsonObj = new JsonParser().parse(input).getAsJsonObject();
        return new StockEntryDto(
                jsonObj.get("symbol").getAsString(),
                jsonObj.get("date").getAsString(),
                jsonObj.get("close").getAsString()
        );
    }

    @SneakyThrows
    private LrcMapperValue convertToValue(StockEntryDto dto) {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");

        LrcMapperValue value = new LrcMapperValue();
        value.setSymbol(dto.getSymbol());
        value.setTimestamp(formatter.parse(dto.getDate()).getTime());
        value.setPrice(Double.parseDouble(dto.getPrice().replace("$", "")));

        return value;
    }
}
