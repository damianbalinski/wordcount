package com.app.mapreduce.lrc;

public class StockEntryDto {
    private final String symbol;
    private final String date;
    private final String price;

    public StockEntryDto(String symbol, String date, String price) {
        this.symbol = symbol;
        this.date = date;
        this.price = price;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getDate() {
        return date;
    }

    public String getPrice() {
        return price;
    }
}
