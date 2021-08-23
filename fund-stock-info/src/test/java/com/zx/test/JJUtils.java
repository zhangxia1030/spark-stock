package com.zx.test;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Description JJUtils function
 *
 * @author zx
 * @version 1.0
 * @date 2021-08-10 19:32
 */
public class JJUtils {

    private static final Random random = new Random();

    public final static String processStockCode(String htmlContents) {
        List<String> entryList = Splitter.on(";").splitToList(htmlContents);
        entryList.forEach(et -> {
            if (et.contains("stockCodesNew")) {
                List<String> equalRight = Splitter.on("=").splitToList(et);
                String str = equalRight.get(1).replaceAll("\\[", "").replaceAll("]", "").replaceAll(";", "").replaceAll("\"", "");
                System.out.println(str);
            }
        });
        return "";
    }

    public final static List<StockInfo> getStockInfoByJJId(String id) {
        Document doc = null;
        try {
            doc = Jsoup.connect("http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code=" + id + "&topline=10&year=&month=&rt=" + random.nextDouble()).get();
        } catch (IOException e) {
            e.printStackTrace();
            return Lists.newArrayList();
        }
        Element elements = doc.getElementsByClass("px12").first();
        //2021-06-30
        if(elements == null) return Lists.newArrayList();
        String dateTime = elements.text();
        System.out.println(dateTime);

        //
        Elements table = doc.select("table").first().select("tbody").select("tr");
        //tr->td
        List<StockInfo> stockInfoList = table.parallelStream().map(element -> {
            List<String> tds = element.select("td").parallelStream().map(e -> e.text()).collect(Collectors.toList());
            StockInfo stockInfo = new StockInfo();
            stockInfo.setDateText(dateTime);
            stockInfo.setJjId(id);
            stockInfo.setCode(tds.get(1));
            stockInfo.setName(tds.get(2));
            stockInfo.setPercent(tds.get(6));
            stockInfo.setOwnCount(tds.get(7));
            stockInfo.setOwnValue(tds.get(8));
            return stockInfo;
        }).collect(Collectors.toList());

        //System.out.println(guPiaoInfoList);
        return stockInfoList;
    }
}
