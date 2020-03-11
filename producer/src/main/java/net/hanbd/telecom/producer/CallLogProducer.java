package net.hanbd.telecom.producer;

import java.io.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 通话日志生成器
 *
 * @author hanbd
 */

public class CallLogProducer {
    private static final int MAX_CALL_SECOND = 30 * 60;

    private List<User> users;
    private LocalDateTime startTime;
    private LocalDateTime endTime;

    public CallLogProducer(List<User> users, LocalDateTime startTime, LocalDateTime endTime) {
        this.users = users;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    /**
     * @param users        user
     * @param startTimeStr startTimeStr, 格式：yyyy-MM-dd'T'HH:mm:ss
     * @param endTimeStr   endTimeStr, 格式同 startTimeStr
     */
    public CallLogProducer(List<User> users, String startTimeStr, String endTimeStr) {
        this.users = users;
        this.startTime = LocalDateTime.parse(startTimeStr);
        this.endTime = LocalDateTime.parse(endTimeStr);
    }

    /**
     * 批量生成日志
     *
     * @param num 日志数量
     * @return callLog list
     */
    public List<CallLog> produce(int num) {
        List<CallLog> logs = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            logs.add(genCallLog());
        }

        return logs;
    }

    /**
     * 将日志写入文件
     *
     * @param file 文件
     * @param logs 日志
     * @return success or not
     */
    public boolean writeTo(File file, List<CallLog> logs) {
        if (!file.getParentFile().exists()) {
            if (file.getParentFile().mkdirs()) {
                return false;
            }
        }
        try (OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(file, true))) {
            for (CallLog log : logs) {
                osw.write(log.toString() + "\n");
                osw.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 生产日志并写入文件
     *
     * @param num  日志条数
     * @param file 文件
     * @return success or not
     */
    public boolean produceAndWrite(int num, File file) {
        List<CallLog> logs = this.produce(num);
        return writeTo(file, logs);
    }

    /**
     * 生成一条通话日志
     *
     * @return callLog
     */
    private CallLog genCallLog() {
        User caller = getUserRandom();
        User callee = getUserRandom();
        while (caller.equals(callee)) {
            callee = getUserRandom();
        }

        LocalDateTime establishTime = generateEstablishTime();
        int duration = (int) (MAX_CALL_SECOND * Math.random());

        return new CallLog(caller, callee, establishTime, duration);
    }

    /**
     * 随机获得用户
     *
     * @return randomUser
     */
    private User getUserRandom() {
        Random random = new Random();
        int index = random.nextInt(this.users.size());
        return this.users.get(index);
    }

    /**
     * 随机生成时间，在给定起止时间之间
     *
     * @return establishTime
     */
    private LocalDateTime generateEstablishTime() {
        Duration d = Duration.between(this.startTime, this.endTime);
        long randomNanos = (long) (d.toNanos() * Math.random());
        return this.startTime.plusNanos(randomNanos);
    }

}
