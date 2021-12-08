package util;

import java.io.File;
import java.util.Objects;

public class FileUtil {

    public static boolean DeleteFile(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < Objects.requireNonNull(children).length; i++) {
                boolean res = DeleteFile(new File(dir, children[i]));
                if (!res) {
                    System.out.println("----------delete failed------------");
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }

}
