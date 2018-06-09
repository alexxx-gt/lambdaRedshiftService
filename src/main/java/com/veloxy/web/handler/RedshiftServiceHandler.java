package com.veloxy.web.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.veloxy.web.model.SalesAnalyticsData;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;

public class RedshiftServiceHandler implements RequestHandler<Map <String, Object>, List<Map<String, Object>>> {

    @Override
    public List<Map<String, Object>> handleRequest(Map <String, Object> request, Context context) {

        if (request.get("query") == null) throw new InvalidParameterException("Query is empty");

        String sql = String.valueOf(request.get("query"));
        Map<String, Object>params = (Map<String, Object>) request.get("params");
        return SalesAnalyticsData.getResult(sql, params);
    }

}
