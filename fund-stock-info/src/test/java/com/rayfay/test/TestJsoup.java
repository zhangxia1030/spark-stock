package com.rayfay.test;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Description TestJsoup function
 *
 * @author zx
 * @version 1.0
 * @date 2021-08-11 11:27
 */
public class TestJsoup {

    @Test
    public void test_1() throws IOException {
        Document doc = Jsoup.connect("http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code=000209&topline=10&year=&month=&rt=0.6969365772720595").get();
        Element elements = doc.getElementsByClass("px12").first();
        //2021-06-30
        String dateTime = elements.text();
        System.out.println(dateTime);

        //
        Elements table = doc.select("table").first().select("tbody").select("tr");
        //tr->td
        List<StockInfo> stockInfoList = table.parallelStream().map(element -> {
            List<String> tds = element.select("td").parallelStream().map(e -> e.text()).collect(Collectors.toList());
            StockInfo stockInfo = new StockInfo();
            stockInfo.setDateText(dateTime);
            stockInfo.setCode(tds.get(1));
            stockInfo.setName(tds.get(2));
            stockInfo.setPercent(tds.get(6));
            stockInfo.setOwnCount(tds.get(7));
            stockInfo.setOwnValue(tds.get(8));
            return stockInfo;
        }).collect(Collectors.toList());

        System.out.println(stockInfoList);
    }
}
