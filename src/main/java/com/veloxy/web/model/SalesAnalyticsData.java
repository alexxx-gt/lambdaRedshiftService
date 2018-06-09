package com.veloxy.web.model;

import com.veloxy.web.utils.AppConstant;

import java.security.InvalidParameterException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SalesAnalyticsData {

    private static Connection getConnection(){
        Connection connection = null;
        try {
            Class.forName(AppConstant.DB_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection = DriverManager.getConnection(AppConstant.REDSHIFT_URL, AppConstant.USERNAME, AppConstant.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;
    }

    public static List<Map<String, Object>> getResult(String sql, Map<String, Object> params){
        PreparedStatement ps = getPreparedStatement(sql, params);
        List<Map<String, Object>> resultSet = new LinkedList<>();

        try {
            ResultSet rs  = ps.executeQuery();

            ResultSetMetaData metaData = rs.getMetaData();

            while (rs.next()){
                Map<String, Object> row = new LinkedHashMap<>();
                for(int i=1; i<=metaData.getColumnCount(); i++){
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                resultSet.add(row);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (NullPointerException e){
            throw new InvalidParameterException("Query is invalid");
        }

        return resultSet;
    }

    private static PreparedStatement getPreparedStatement(String sql, Map<String, Object> params){
        PreparedStatement ps = null;
        try {
            if (params != null){
                for(Map.Entry<String, Object> entry : params.entrySet())
                    sql = sql.replace(":"+entry.getKey(), "'"+String.valueOf(entry.getValue())+"'");
            }

            if(sql.indexOf(':') > 0) throw new InvalidParameterException("Not enough params according to query");

            ps = getConnection().prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ps;
    }

}
