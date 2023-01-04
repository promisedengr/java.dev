import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;


public class FTM {
    public static void main(String[] args) {
        // String connURL = args[0];
        // String userName = args[1];
        // String password = args[2];
        String connURL = "jdbc:postgresql://localhost:5432/pa2.db";
        String userName = "postgres";
        String password = "";

        connectAndTest(connURL, userName, password);
    }
    private static final String createInfluenceTable = "CREATE TABLE influence " +
        "(ID INT PRIMARY KEY ," +
        " Who VARCHAR(550), " +
        " Whom VARCHAR(550))";
    
    public static void connectAndTest(String connURL, String userName, String password) {
        try {
            Connection conn = DriverManager.getConnection(connURL, userName, password);
            DatabaseMetaData dbm = conn.getMetaData();
            // check if "employee" table is there
            ResultSet tables = dbm.getTables(null, null, "influence", null);
            if (tables.next()) {
            // Table exists
                System.out.println("exists");
                Statement stmt = conn.createStatement();
                String query = "Drop table influence";
                stmt.execute(query);
            } else {
                System.out.println("not exist");
            }
            getInfluenceTableData(conn);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void getInfluenceTableData(Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM transfer");
            System.out.println("connected");
            List<String> whoList = new ArrayList<String>();
            List<String> whomList = new ArrayList<String>();
            while (rs.next()) {
                Statement stmtForWho = conn.createStatement();
                ResultSet rsForWho = stmtForWho.executeQuery("SELECT * FROM depositor WHERE ano = '" + rs.getString("src") +"'");
                while (rsForWho.next()) {
                    whoList.add(rsForWho.getString("cname"));
                }
                Statement stmtForWhom = conn.createStatement();
                ResultSet rsForWhom = stmtForWhom.executeQuery("SELECT * FROM depositor WHERE ano = '" + rs.getString("tgt") +"'");
                while (rsForWhom.next()) {
                    whomList.add(rsForWhom.getString("cname"));
                }
            }
            createInfluenceTable(conn, whoList, whomList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void createInfluenceTable(Connection conn, List<String> whoList, List<String> whomList) {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(createInfluenceTable);
            for( int i=0 ;i<whoList.size(); i++){  
                insertDataToInfluenceTable(conn, i + 1, whoList.get(i), whomList.get(i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void insertDataToInfluenceTable(Connection conn, int id, String who, String whom) {
        try {

            PreparedStatement st = conn.prepareStatement("INSERT INTO influence (ID, Who, Whom) VALUES (?, ?, ?)");
            st.setInt(1, id);
            st.setString(2, who);
            st.setString(3, whom);
            st.executeUpdate();
            st.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
