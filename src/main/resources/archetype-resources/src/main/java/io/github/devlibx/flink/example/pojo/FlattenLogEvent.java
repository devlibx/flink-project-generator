#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package io.github.devlibx.flink.example.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.gitbub.devlibx.easy.helper.common.LogEvent;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.Data;
import org.joda.time.DateTime;

import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class FlattenLogEvent {
    private String id;
    private String idType;
    private String service;
    private String eventType;
    private String eventSubType;
    private String entityType;
    private String entityId;
    private String userId;
    private String status;
    private Map<String, String> dimensions;
    private int year;
    private int month;
    private int day;
    private int hour;
    private int min;
    private long timestamp;

    @JsonProperty("udf_str_1")
    private String udfStr1;
    @JsonProperty("udf_str_2")
    private String udfStr2;
    @JsonProperty("udf_str_3")
    private String udfStr3;
    @JsonProperty("udf_str_4")
    private String udfStr4;
    @JsonProperty("udf_str_5")
    private String udfStr5;
    @JsonProperty("udf_long_1")
    private Long udfLong1;
    @JsonProperty("udf_long_2")
    private Long udfLong2;
    @JsonProperty("udf_long_3")
    private Long udfLong3;
    @JsonProperty("udf_long_4")
    private Long udfLong4;
    @JsonProperty("udf_long_5")
    private Long udfLong5;
    @JsonProperty("udf_bool_1")
    private Boolean udfBool1;
    @JsonProperty("udf_bool_2")
    private Boolean udfBool2;
    @JsonProperty("udf_bool_3")
    private Boolean udfBool3;
    @JsonProperty("udf_bool_4")
    private Boolean udfBool4;
    @JsonProperty("udf_bool_5")
    private Boolean udfBool5;
    @JsonProperty("udf_double_1")
    private Double udfDouble1;
    @JsonProperty("udf_double_2")
    private Double udfDouble2;
    @JsonProperty("udf_double_3")
    private Double udfDouble3;
    @JsonProperty("udf_double_4")
    private Double udfDouble4;
    @JsonProperty("udf_double_5")
    private Double udfDouble5;


    @JsonIgnore
    public String key() {
        return id + "-" + idType;
    }

    public static FlattenLogEvent convert(LogEvent logEvent) {
        FlattenLogEvent ev = new FlattenLogEvent();
        if (logEvent.getEntity() != null) {
            ev.id = logEvent.getEntity().getId();
            ev.idType = logEvent.getEntity().getType();
            ev.entityId = logEvent.getEntity().getId();
            ev.entityType = logEvent.getEntity().getType();
        }
        ev.service = logEvent.getService();
        ev.eventType = logEvent.getEventType();
        ev.eventSubType = logEvent.getEventSubType();
        ev.dimensions = logEvent.getDimensions();
        ev.timestamp = logEvent.getTimestamp();
        DateTime dt = new DateTime(ev.timestamp);
        ev.year = dt.getYear();
        ev.month = dt.getMonthOfYear();
        ev.day = dt.getDayOfMonth();
        ev.hour = dt.getHourOfDay();
        ev.min = dt.getHourOfDay();

        if (logEvent.getData() != null) {
            ev.userId = logEvent.getData().getString("user_id", "");
            ev.udfLong1 = logEvent.getData().getLong("udf_long_1");
            ev.udfLong2 = logEvent.getData().getLong("udf_long_2");
            ev.udfLong3 = logEvent.getData().getLong("udf_long_3");
            ev.udfLong4 = logEvent.getData().getLong("udf_long_4");
            ev.udfLong5 = logEvent.getData().getLong("udf_long_5");

            ev.udfDouble1 = logEvent.getData().getDouble("udf_double_1");
            ev.udfDouble2 = logEvent.getData().getDouble("udf_double_2");
            ev.udfDouble3 = logEvent.getData().getDouble("udf_double_3");
            ev.udfDouble4 = logEvent.getData().getDouble("udf_double_4");
            ev.udfDouble5 = logEvent.getData().getDouble("udf_double_5");

            ev.udfBool1 = logEvent.getData().getBoolean("udf_bool_1");
            ev.udfBool2 = logEvent.getData().getBoolean("udf_bool_2");
            ev.udfBool3 = logEvent.getData().getBoolean("udf_bool_3");
            ev.udfBool4 = logEvent.getData().getBoolean("udf_bool_4");
            ev.udfBool5 = logEvent.getData().getBoolean("udf_bool_5");

            ev.udfStr1 = logEvent.getData().getString("udf_str_1");
            ev.udfStr2 = logEvent.getData().getString("udf_str_2");
            ev.udfStr3 = logEvent.getData().getString("udf_str_3");
            ev.udfStr4 = logEvent.getData().getString("udf_str_4");
            ev.udfStr5 = logEvent.getData().getString("udf_str_5");
        }
        return ev;
    }

    public static FlattenLogEvent merge(FlattenLogEvent original, FlattenLogEvent newEvent) {
        StringObjectMap o = JsonUtils.convertAsStringObjectMap(JsonUtils.asJson(original));
        StringObjectMap n = JsonUtils.convertAsStringObjectMap(JsonUtils.asJson(newEvent));
        o.putAll(n);
        return JsonUtils.readObject(JsonUtils.asJson(o), FlattenLogEvent.class);
    }
}
