#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.gitbub.devlibx.easy.helper.common.LogEvent;
import lombok.Data;
import org.joda.time.DateTime;

import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlattenLogEvent {
    private String id;
    private String idType;
    private String service;
    private String eventType;
    private String eventSubType;
    private String entityType;
    private String entityId;
    private String status;
    private Map<String, String> dimensions;
    private int year;
    private int month;
    private int day;
    private int hour;
    private int min;
    private long timestamp;
    private String udfStr1;
    private String udfStr2;
    private String udfStr3;
    private String udfStr4;
    private String udfStr5;
    private long udfLong1;
    private long udfLong2;
    private long udfLong3;
    private long udfLong4;
    private long udfLong5;
    private int udfBool1;
    private int udfBool2;
    private int udfBool3;
    private int udfBool4;
    private int udfBool5;
    private int udfDouble1;
    private int udfDouble2;
    private int udfDouble3;
    private int udfDouble4;
    private int udfDouble5;


    public static FlattenLogEvent convert(LogEvent logEvent) {
        FlattenLogEvent ev = new FlattenLogEvent();
        ev.id = logEvent.getEntity().getId();
        ev.idType = logEvent.getEntity().getType();
        ev.service = logEvent.getService();
        ev.eventType = logEvent.getEventType();
        ev.eventSubType = logEvent.getEventSubType();
        ev.entityId = logEvent.getEntity().getId();
        ev.entityType = logEvent.getEntity().getType();
        ev.dimensions = logEvent.getDimensions();
        ev.timestamp = logEvent.getTimestamp();
        DateTime dt = new DateTime(ev.timestamp);
        ev.year = dt.getYear();
        ev.month = dt.getMonthOfYear();
        ev.day = dt.getDayOfMonth();
        ev.hour = dt.getHourOfDay();
        ev.min = dt.getHourOfDay();
        return ev;
    }
}
