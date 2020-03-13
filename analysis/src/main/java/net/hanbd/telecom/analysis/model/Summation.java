package net.hanbd.telecom.analysis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.mortbay.log.Log;

/**
 * 汇总表
 *
 * @author hanbd
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Log4j
public class Summation {
    private String idDateUser;
    private int idDate;
    private int idUser;
    private int callSum;
    private int callDurationSum;

    public Summation(int idDate, int idUser, int callSum, int callDurationSum) {
        this.idDateUser = idDate + "_" + idUser;
        this.idDate = idDate;
        this.idUser = idUser;
        this.callSum = callSum;
        this.callDurationSum = callDurationSum;
        Log.info("summationPojo: " + this.toString());
    }
}
