package com.zx.test;

/**
 * Description GuPiaoInfo function
 *
 * @author zx
 * @version 1.0
 * @date 2021-08-11 11:30
 */
public class StockInfo {
    private String jjId;//截止至日期
    private String dateText;//截止至日期
    private String code;//股票代码
    private String name;//股票名称
    private String percent;//占净值比例
    private String ownCount;//持股数（万股）
    private String ownValue;//持仓市值（万元）

    public String getJjId() {
        return jjId;
    }

    public void setJjId(String jjId) {
        this.jjId = jjId;
    }

    public String getDateText() {
        return dateText;
    }

    public void setDateText(String dateText) {
        this.dateText = dateText;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPercent() {
        return percent;
    }

    public void setPercent(String percent) {
        this.percent = percent;
    }

    public String getOwnCount() {
        return ownCount;
    }

    public void setOwnCount(String ownCount) {
        this.ownCount = ownCount;
    }

    public String getOwnValue() {
        return ownValue;
    }

    public void setOwnValue(String ownValue) {
        this.ownValue = ownValue;
    }

    @Override
    public String toString() {
        return "GuPiaoInfo{" +
                "jjId='" + jjId + '\'' +
                ", dateText='" + dateText + '\'' +
                ", code='" + code + '\'' +
                ", name='" + name + '\'' +
                ", percent='" + percent + '\'' +
                ", ownCount='" + ownCount + '\'' +
                ", ownValue='" + ownValue + '\'' +
                '}';
    }

}
