package com.rayfay.daas.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by zx on 2019/11/9 - 16:47.
 */
public class HttpCollectUtils {

    private final static Logger logger = LoggerFactory.getLogger(HttpCollectUtils.class);

    public final static String buildParameters(Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        List<String> collect = map.entrySet().parallelStream().map((Map.Entry entry) ->
                new StringBuffer((String) entry.getKey()).append("=").append(entry.getValue()).toString()
        ).collect(Collectors.toList());

        return StringUtils.join(collect, "&");
    }

    public final static Header[] buildHeaders(Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        List<BasicHeader> collect = map.entrySet().parallelStream().map((Map.Entry<String, Object> entry) ->
                new BasicHeader(entry.getKey(), entry.getKey())
        ).collect(Collectors.toList());

        return (Header[]) collect.toArray();
    }
}
