package org.senseml;

import org.senseml.util.FileUtil;

import java.util.List;

/**
 * DateSet
 * Created by xueyintao on 2018/3/31.
 */
public class DateSet {

    /**
     * 获得订单数据
     * 字段: date,user_id,cost,recharge
     *
     * @return
     */
    public static List<String> getOrdersData() {
        String filePath = DateSet.class.getResource("/orders.txt").getPath();
        System.out.println("filePath:" + filePath);

        return FileUtil.readFile(filePath);
    }

}
