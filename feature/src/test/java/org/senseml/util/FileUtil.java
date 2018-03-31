package org.senseml.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * FileUtil
 * Created by xueyintao on 2018/3/31.
 */
public class FileUtil {

    /**
     * 将文件读取为List
     *
     * @param filePath
     * @return
     */
    public static List<String> readFile(String filePath) {
        List<String> list = new ArrayList<String>();
        if (null == filePath || filePath.length() == 0) {
            return list;
        }

        BufferedReader br = null;
        try {
            File file = new File(filePath);
            br = new BufferedReader(new FileReader(file));
            String line;
            while((line = br.readLine()) != null) {
                list.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

}
