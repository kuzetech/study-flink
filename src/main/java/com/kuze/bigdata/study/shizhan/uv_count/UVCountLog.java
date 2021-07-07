package com.kuze.bigdata.study.shizhan.uv_count;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UVCountLog {

    private static ObjectMapper mapper = new ObjectMapper();

    String uid;
    Long ts;

    public static UVCountLog parseLog(String line) throws JsonProcessingException {
        return mapper.readValue(line, UVCountLog.class);
    }
}
