#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.pojo;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

/**
 * Event deceiver to convert Kafka message to POJO. It can be copied for other Pojos
 */
public class EventDeserializationSchema implements DeserializationSchema<Order> {
    private static final long serialVersionUID = 1L;

    @Override
    public Order deserialize(byte[] message) {
        String line = new String(message, StandardCharsets.UTF_8);
        return JsonUtils.readObject(line, Order.class);
    }

    @Override
    public boolean isEndOfStream(Order nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}
