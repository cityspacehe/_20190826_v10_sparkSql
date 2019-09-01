/**
 * @author hq
 * @date 2019-08-31
 */

import javax.print.DocFlavor;
import java.sql.*;

/**
 *hive 连接jdbc  org.apache.hive.jdbc.HiveDriver
 * jdbc:hive2://192.168.130.111:10000/数据库名
 */
public class HiveConnTest {
    public static void main(String[] args) {
        String driver="org.apache.hive.jdbc.HiveDriver";
        String URL="jdbc:hive2://192.168.130.111:10000/works";
        // jdbc:hive2://192.168.0.1:10000/default", "hive", ""
        String userName="";
        String password="";
        try {
            Class.forName(driver);
            Connection conn= DriverManager.getConnection(URL,userName,password);
            String sql="select *from score";
            PreparedStatement ps =conn.prepareStatement(sql);
            ResultSet res = ps.executeQuery();
            while (res.next()){
                String sno=res.getString(1);
                String cno=res.getString(2);
                String score=res.getString(3);
                System.out.println(sno+" "+cno+" "+score);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
