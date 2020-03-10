package producer;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author hanbd
 */
@Data
@AllArgsConstructor
public class User {
    private int id;
    private String phone;
    private String name;
}
