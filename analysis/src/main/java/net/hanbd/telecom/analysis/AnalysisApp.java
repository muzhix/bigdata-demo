package net.hanbd.telecom.analysis;

import net.hanbd.telecom.analysis.mr.runner.SummationRunner;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author hanbd
 */
public class AnalysisApp {
    /**
     * 使用定义的mr进行离线处理
     */
    private static void runMr(String[] args) {
        try {
            int status = ToolRunner.run(SummationRunner.getInstance(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        runMr(args);
    }
}
