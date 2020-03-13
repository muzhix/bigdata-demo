package net.hanbd.telecom.analysis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 时间维度表
 *
 * @author hanbd
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Date {
    private int id;
    private int year;
    private int month;
    private int day;
}
