import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
 
public class SampleSQLRead {
 
    public static void main(String a[]){
         
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Properties prop = new Properties();
            
            Connection con = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/test","root","");
            Statement stmt = con.createStatement();
            System.out.println("Created DB Connection....");
            ResultSet rs = stmt.executeQuery("select * from places");
            while(rs.next()){
                System.out.println(rs.getString("Country"));
                System.out.println(rs.getString("City"));
            }
            rs.close();
            con.close();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
