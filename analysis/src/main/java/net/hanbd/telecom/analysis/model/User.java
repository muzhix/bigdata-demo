package net.hanbd.telecom.analysis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户表
 *
 * @author hanbd
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private int id;
    private String phone;
    private String name;
}
