// imports
import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;

class Tuple {
    /**
     * class that represents tuple
     */
    public String first;
    public String second;
    public double third;

    public Tuple(String first, String second, double third) {
        // Constructor for Tuple class.
        this.first = first;
        this.second = second;
        this.third = third;
    }
}

public class Assigment{
    private String connection;

    public Assigment(String DB_username, String DB_password) {
        /**
         * Constructor for Assigment class. Sets the DB_username and DB_password
         * and builds the connection string.
         */
        String connectionUrl = "jdbc:sqlserver://" + "132.72.64.124" + ":1433;databaseName=" + DB_username + ";user=" + DB_username + ";password=" + DB_password + ";encrypt=false;";
        connection = connectionUrl;
    }
    public void fileToDataBase(String path) throws SQLException, ClassNotFoundException {
        /**
         * Reads a CSV file and writes the data to a database table.
         */
        Connection conn = null;
        PreparedStatement pstmt = null;
        int cnt_MID = 0;
        // declaring query
        String INSERT_QUERY = "INSERT INTO Mediaitems(MID, PROD_YEAR, TITLE) VALUES (?, ?, ?)";
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line = "";
            String cvsSplitBy = ",";

            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conn = DriverManager.getConnection(connection);

            pstmt = conn.prepareStatement(INSERT_QUERY);

            while ((line = br.readLine()) != null) {
                cnt_MID++;
                String[] values = line.split(cvsSplitBy);

                String title = values[0];
                int prodYear = Integer.parseInt(values[1]);

                pstmt.setInt(1, cnt_MID);
                pstmt.setInt(2, prodYear);
                pstmt.setString(3, title);

                pstmt.execute();
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Calculates the similarity between media items and stores the results in the Similarity table of the database.
     * This method uses stored procedures defined in the database to calculate the similarity.
     *
     * @throws SQLException if a database access error occurs.
     */
    public void calculateSimilarity() throws SQLException {
        PreparedStatement pstmt = null;
        Connection con = null;
        int num_of_rows = 0;
        try {
            // Load SQL Server JDBC driver
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            // Connect to the database
            con = DriverManager.getConnection(connection);
            // Create a statement to count the number of rows in the Mediaitems table
            Statement stmt = con.createStatement();
            String countQuery = "SELECT COUNT(*) FROM Mediaitems";
            ResultSet rs = stmt.executeQuery(countQuery);
            // Get the number of rows in the Mediaitems table
            if (rs.next()) {
                num_of_rows = rs.getInt(1);
            }
            // Call the stored procedure dbo.MaximalDistance to get the maximum distance between media items
            CallableStatement cstmt_max = con.prepareCall("{? = call dbo.MaximalDistance()}");
            cstmt_max.registerOutParameter(1, Types.INTEGER);
            cstmt_max.execute();
            int outputValueMax = cstmt_max.getInt(1);

            // Call the stored procedure dbo.SimCalculation to calculate the similarity between each pair of media items
            CallableStatement cstmt = con.prepareCall("{? = call dbo.SimCalculation(?, ?, ?)}");
            for (int MID1 = 1; MID1 <= num_of_rows; MID1++) {
                for (int MID2 = MID1 + 1; MID2 <= num_of_rows; MID2++) {
                    cstmt.setInt(2, MID1);
                    cstmt.setInt(3, MID2);
                    cstmt.setInt(4, outputValueMax);

                    // Register the output parameter to get the calculated similarity value
                    cstmt.registerOutParameter(1, Types.DOUBLE);
                    cstmt.execute();
                    double outputValue = cstmt.getDouble(1);

                    // Insert the calculated similarity value into the Similarity table
                    pstmt = con.prepareStatement("INSERT INTO Similarity(MID1, MID2, SIMILARITY) VALUES (?, ?, ?)");
                    pstmt.setInt(1, MID1);
                    pstmt.setInt(2, MID2);
                    pstmt.setDouble(3, outputValue);
                    pstmt.execute();
                }
            }
        } catch (SQLException | ClassNotFoundException e) {
            // Handle any exceptions that occur during database access
            e.printStackTrace();
        } finally {
            try {
                // Close the PreparedStatement and Connection objects
                if (pstmt != null) {
                    pstmt.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Returns the title of a media item with a given MID.
     *
     * @param mid the ID of the media item
     * @return the title of the media item
     * @throws SQLException           if a database access error occurs
     * @throws ClassNotFoundException if the Microsoft SQL Server JDBC driver is not found
     */
    public String returntitle(long mid) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conn = DriverManager.getConnection(connection);

            String title = "";

            String mid_title_query = "SELECT TITLE FROM Mediaitems WHERE MID=?";
            pstmt = conn.prepareStatement(mid_title_query);
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                title = rs.getString("TITLE");
            }
            return title;
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return "";
    }

    /**
     * Prints the titles and similarities of media items that are similar to the given media item.
     *
     * @param mid The MID of the media item to find similarities for.
     * @throws SQLException If there is an error accessing the database.
     */
    public void printSimilarities(long mid) throws SQLException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            // Connect to the database
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conn = DriverManager.getConnection(connection);

            // Get the number of similar media items
            int num_of_rows = 0;
            String countQuery = "SELECT COUNT(*) FROM [Similarity] WHERE (MID1=? OR MID2=?) AND SIMILARITY>0.3";
            pstmt = conn.prepareStatement(countQuery);
            pstmt.setLong(1, mid);
            pstmt.setLong(2, mid);

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                num_of_rows = rs.getInt(1);
            }

            // Create an array of Tuples to hold the similar media items
            Tuple[] tuples = new Tuple[num_of_rows];

            // Get the title of the given media item
            String title = returntitle(mid);

            // Get the similar media items and their similarities
            String mid_sim_query = "SELECT * FROM [Similarity] WHERE (MID1=? OR MID2=?) AND SIMILARITY>0.3";
            pstmt = conn.prepareStatement(mid_sim_query);

            int i = 0;
            pstmt.setLong(1, mid);
            pstmt.setLong(2, mid);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                double sim = rs.getDouble("SIMILARITY");
                long MID1 = rs.getLong("MID1");
                long MID2 = rs.getLong("MID2");
                if (sim >= 0.3) {
                    // If MID1 matches the given MID, create a Tuple with MID2 and the similarity
                    if (MID1 == mid)
                        tuples[i] = new Tuple(title, returntitle(MID2), sim);
                    // If MID2 matches the given MID, create a Tuple with MID1 and the similarity
                    if (MID2 == mid)
                        tuples[i] = new Tuple(title, returntitle(MID1), sim);
                    i++;
                }
            }

            // Sort the array of Tuples by similarity
            Arrays.sort(tuples, new Comparator<Tuple>() {
                public int compare(Tuple a, Tuple b) {
                    return Double.compare(a.third, b.third);
                }
            });

            // Print the sorted array of Tuples
            for (int j = 0; j < tuples.length; j++) {
                System.out.println(tuples[j].first + ", " + tuples[j].second + ", " + tuples[j].third);
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            // Close the PreparedStatement and Connection
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}


