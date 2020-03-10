package producer;

import com.google.common.base.Joiner;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author hanbd
 */
@Data
@AllArgsConstructor
public class CallLog {
    private User from;
    private User to;
    private LocalDateTime establishTime;
    /**
     * 通话时间，second
     */
    private int duration;

    @Override
    public String toString() {
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        Joiner joiner = Joiner.on(",").skipNulls();
        return joiner.join(from.getPhone(), to.getPhone(), df.format(establishTime), String.valueOf(duration));
    }


}
