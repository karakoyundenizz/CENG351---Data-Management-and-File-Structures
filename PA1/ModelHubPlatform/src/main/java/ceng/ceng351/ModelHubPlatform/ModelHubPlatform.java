package ceng.ceng351.ModelHubPlatform;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ModelHubPlatform implements IModelHubPlatform {

    private Connection connection;
    // The Evaluation class creates the actual H2 database connection.
    // This class does NOT open a new connection itself, it only stores and uses the connection passed from Evaluation.

    @Override
    public void initialize(Connection connection) {
        // Assign the shared DB connection so all SQL methods use the same database session.
        this.connection = connection;
    }

    //Drop tables
    @Override
    public int dropTables() {
        int tableCount = 0;

        // Order matters: tables that are referenced by others must be dropped last to avoid foreign key constraint errors.
        String[] tables = new String[]{
                "includes",       // References Publications, Results
                "Results",        // References runs
                "runs",           // References Users, ModelVersions, Datasets
                "designed_for",   // References Models, Tasks
                "ModelVersions",  // References Models
                "uploads",        // References Users, Datasets
                "Models",         // References Organizations
                "follows",        // References Users
                "Profiles",       // References Users
                "Publications",   // Referenced by includes
                "Datasets",       // Referenced by uploads, runs
                "Tasks",          // Referenced by designed_for
                "Organizations",  // Referenced by Models
                "Users"           // Referenced by Profiles, follows, runs, Results, uploads
        };

        for (String table : tables) {
            try {
                Statement statement = this.connection.createStatement();
                String dropTableSQL = "DROP TABLE IF EXISTS " + table + ";";
                statement.executeUpdate(dropTableSQL);
                tableCount++;
                statement.close();
            } catch (SQLException e) {
                //System.out.println("Failed to drop " + table + ": " + e.getMessage());
            }
        }
        return tableCount;
    }


    //6.1 Task 1: Create Database Tables
    @Override
    public int createTables() {
        int tableCount = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/

        // for database connection
       try{
           Statement statement = this.connection.createStatement();

           // Users
           String createTableSQL = "CREATE TABLE IF NOT EXISTS Users(" +
                   "PIN INT," +
                   "user_name VARCHAR(255)," +
                   "reputation_score INT," +
                   "PRIMARY KEY (PIN)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;

           // Organizations
           createTableSQL = "CREATE TABLE IF NOT EXISTS Organizations(" +
                   "OrgID INT," +
                   "org_name VARCHAR(255)," +
                   "rating DOUBLE," +
                   "PRIMARY KEY (OrgID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;



           // Tasks
           createTableSQL = "CREATE TABLE IF NOT EXISTS Tasks(" +
                   "TaskID INT," +
                   "task_name VARCHAR(255)," +
                   "PRIMARY KEY (TaskID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;

           // Datasets
           createTableSQL = "CREATE TABLE IF NOT EXISTS Datasets(" +
                   "DatasetID INT," +
                   "dataset_name VARCHAR(255)," +
                   "modality VARCHAR(255)," +
                   "number_of_rows INT," +
                   "PRIMARY KEY (DatasetID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;

           //Publications
           createTableSQL = "CREATE TABLE IF NOT EXISTS Publications(" +
                   "PubID INT," +
                   "title VARCHAR(255)," +
                   "venue VARCHAR(255)," +
                   "PRIMARY KEY (PubID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;


           //Profiles
           createTableSQL = "CREATE TABLE IF NOT EXISTS Profiles(" +
                   "ProfileID INT," +
                   "bio VARCHAR(255)," +
                   "avatar_url VARCHAR(255)," +
                   "PIN INT NOT NULL UNIQUE," +
                   "PRIMARY KEY (ProfileID)," +
                   "FOREIGN KEY (PIN) REFERENCES Users(PIN)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;



           //follows
           createTableSQL = "CREATE TABLE IF NOT EXISTS follows (" +
                   "followerPIN INT," +
                   "followeePIN INT," +
                   "following_date DATE," +
                   "PRIMARY KEY (followerPIN, followeePIN)," +
                   "FOREIGN KEY (followerPIN) REFERENCES Users(PIN)," +
                   "FOREIGN KEY (followeePIN) REFERENCES Users(PIN)," +
                   "CHECK (followerPIN <> followeePIN)" +  // for checking inequality
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;


           //Models
           createTableSQL = "CREATE TABLE IF NOT EXISTS Models(" +
                   "ModelID INT," +
                   "model_name VARCHAR(255)," +
                   "license VARCHAR(255)," +
                   "size VARCHAR(255)," +
                   "OrgID INT NOT NULL," +
                   "PRIMARY KEY (ModelID)," +
                   "FOREIGN KEY (OrgID) REFERENCES Organizations(OrgID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;

           //uploads
           createTableSQL = "CREATE TABLE IF NOT EXISTS uploads(" +
                   "PIN INT," +
                   "DatasetID INT," +
                   "role VARCHAR(255)," +
                   "PRIMARY KEY (PIN, DatasetID)," +
                   "FOREIGN KEY (PIN) REFERENCES Users(PIN)," +
                   "FOREIGN KEY (DatasetID) REFERENCES Datasets(DatasetID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;


           //ModelVersions
           createTableSQL = "CREATE TABLE IF NOT EXISTS ModelVersions(" +
                   "ModelID INT," +
                   "version_no VARCHAR(255)," +
                   "version_date DATE," +
                   "PRIMARY KEY (ModelID, version_no)," +
                   "FOREIGN KEY (ModelID) REFERENCES Models(ModelID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;

           //designed_for
           createTableSQL = "CREATE TABLE IF NOT EXISTS designed_for(" +
                   "ModelID INT," +
                   "TaskID INT," +
                   "is_primary BOOL," +
                   "PRIMARY KEY (ModelID, TaskID)," +
                   "FOREIGN KEY (ModelID) REFERENCES Models(ModelID)," +
                   "FOREIGN KEY (TaskID) REFERENCES Tasks(TaskID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;

           //runs
           createTableSQL = "CREATE TABLE IF NOT EXISTS runs(" +
                   "PIN INT," +
                   "ModelID INT," +
                   "version_no VARCHAR(255)," +
                   "DatasetID INT," +
                   "run_type VARCHAR(255)," +
                   "PRIMARY KEY (PIN, ModelID,version_no, DatasetID)," +
                   "FOREIGN KEY (PIN) REFERENCES Users(PIN)," +
                   "FOREIGN KEY (ModelID,version_no) REFERENCES ModelVersions(ModelID,version_no)," +
                   "FOREIGN KEY (DatasetID) REFERENCES Datasets(DatasetID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;

           //Results
           createTableSQL = "CREATE TABLE IF NOT EXISTS Results(" +
                   "ResultID INT," +
                   "accuracy DOUBLE," +
                   "f1_score DOUBLE," +
                   "hyperparameter_config VARCHAR(255)," +
                   "PIN INT NOT NULL," +
                   "ModelID INT NOT NULL," +
                   "version_no VARCHAR(255) NOT NULL," +
                   "DatasetID INT NOT NULL," +
                   "PRIMARY KEY (ResultID)," +
                   "FOREIGN KEY (PIN,ModelID,version_no, DatasetID) REFERENCES runs(PIN,ModelID,version_no, DatasetID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;

           // includes
           createTableSQL = "CREATE TABLE IF NOT EXISTS includes(" +
                   "PubID INT," +
                   "ResultID INT," +
                   "placement_type VARCHAR(255)," +
                   "placement_section VARCHAR(255)," +
                   "PRIMARY KEY (PubID, ResultID)," +
                   "FOREIGN KEY (PubID) REFERENCES Publications(PubID)," +
                   "FOREIGN KEY (ResultID) REFERENCES Results(ResultID)" +
                   ")" ;
           statement.executeUpdate(createTableSQL);
           tableCount++;

           // first independent tables (Users, Organizations, Tasks, Datasets,
           //Publications), then tables referencing them (Profiles, follows, Models, uploads),
           //followed by model-dependent tables (ModelVersions, designed for), then run-dependent
           //tables (runs), and finally tables referencing results (Results, includes)
           statement.close();
       }
       catch (SQLException e){
//           System.out.println("Task1 -> Failed to connect to database "  + ": " + e.getMessage());
       }

        return tableCount;
    }


    //6.2 Task 2: Insert Users
    @Override
    public int insertUsers(User[] users) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/

        // for database connection
        try {
            String query = "INSERT INTO Users (PIN, user_name, reputation_score) VALUES (?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(User user: users){
                try{
                    statement.setInt(1, user.getPIN());
                    statement.setString(2, user.getUser_name());
                    statement.setInt(3, user.getReputation_score());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert User: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in User"  + ": " + e.getMessage());
        }
        
        return rowsInserted;
    }

    //6.2 Task 2: Insert Organizations
    @Override
    public int insertOrganizations(Organization[] organizations) {
        int rowsInserted = 0;
        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        // for database connection
        try {
            String query = "INSERT INTO Organizations (OrgID, org_name, rating) VALUES (?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(Organization organization: organizations){
                try{
                    statement.setInt(1, organization.getOrgID());
                    statement.setString(2, organization.getOrg_name());
                    statement.setDouble(3, organization.getRating());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert Organization: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in Organization"  + ": " + e.getMessage());
        }


        return rowsInserted;
    }

    //6.2 Task 2: Insert Tasks
    @Override
    public int insertTasks(Task[] tasks) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        // for database connection
        try {
            String query = "INSERT INTO Tasks (TaskID, task_name) VALUES (?, ?)";
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(Task task: tasks){
                try{
                    statement.setInt(1, task.getTaskID());
                    statement.setString(2, task.getTask_name());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert Task: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in Task"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert Datasets
    @Override
    public int insertDatasets(Dataset[] datasets) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO Datasets (DatasetID, dataset_name, modality, number_of_rows) VALUES (?, ?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(Dataset dataset: datasets){
                try{
                    statement.setInt(1, dataset.getDatasetID());
                    statement.setString(2, dataset.getDataset_name());
                    statement.setString(3, dataset.getModality());
                    statement.setInt(4, dataset.getNumber_of_rows());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert Dataset: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in Dataset"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert Publications
    @Override
    public int insertPublications(Publication[] publications) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/


        // for database connection
        try {
            String query = "INSERT INTO Publications (PubID, title, venue) VALUES (?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(Publication publication: publications){
                try{
                    statement.setInt(1, publication.getPubID());
                    statement.setString(2, publication.getTitle());
                    statement.setString(3, publication.getVenue());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert Publication: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in Publication"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert Profiles
    @Override
    public int insertProfiles(Profile[] profiles) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO Profiles (ProfileID, bio, avatar_url, PIN) VALUES (?, ?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(Profile profile: profiles){
                try{
                    statement.setInt(1, profile.getProfileID());
                    statement.setString(2, profile.getBio());
                    statement.setString(3, profile.getAvatar_url());
                    statement.setInt(4, profile.getPIN());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert Profile: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in Profile"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert follows
    @Override
    public int insertfollows(follow[] follows) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO follows (followerPIN, followeePIN, following_date) VALUES (?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(follow follow: follows){
                try{
                    statement.setInt(1, follow.getFollowerPIN());
                    statement.setInt(2, follow.getFolloweePIN());
                    statement.setDate(3, Date.valueOf(follow.getFollowing_date()));
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert follow: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in follow"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert Models
    @Override
    public int insertModels(Model[] models) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO Models (ModelID, model_name, license, size, OrgID) VALUES (?, ?, ?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(Model model: models){
                try{
                    statement.setInt(1, model.getModelID());
                    statement.setString(2, model.getModel_name());
                    statement.setString(3, model.getLicense());
                    statement.setString(4, model.getSize());
                    statement.setInt(5, model.getOrgID());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert Model: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in Model"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert uploads
    @Override
    public int insertuploads(upload[] uploads) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO uploads (PIN, DatasetID, role) VALUES (?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(upload upload: uploads){
                try{
                    statement.setInt(1, upload.getPIN());
                    statement.setInt(2, upload.getDatasetID());
                    statement.setString(3, upload.getRole());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert upload: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in upload"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert ModelVersions
    @Override
    public int insertModelVersions(ModelVersion[] modelVersions) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO ModelVersions (ModelID, version_no, version_date) VALUES (?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(ModelVersion modelVersion: modelVersions){
                try{
                    statement.setInt(1, modelVersion.getModelID());
                    statement.setString(2, modelVersion.getVersion_no());
                    statement.setDate(3, Date.valueOf(modelVersion.getVersion_date()));
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert ModelVersion: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in ModelVersion"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert designed_for
    @Override
    public int insertdesigned_fors(designed_for[] designedFors) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO designed_for (ModelID, TaskID, is_primary) VALUES (?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(designed_for designed_for : designedFors){
                try{
                    statement.setInt(1, designed_for.getModelID());
                    statement.setInt(2, designed_for.getTaskID());
                    statement.setBoolean(3, designed_for.isIs_primary());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert designed_for: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in designed_for"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert runs
    @Override
    public int insertruns(run[] runs) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO runs (PIN, ModelID, version_no, DatasetID, run_type) VALUES (?, ?, ?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(run run: runs){
                try{
                    statement.setInt(1, run.getPIN());
                    statement.setInt(2, run.getModelID());
                    statement.setString(3, run.getVersion_no());
                    statement.setInt(4, run.getDatasetID());
                    statement.setString(5, run.getRun_type());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert run: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in runs"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert Results
    @Override
    public int insertResults(Result[] results) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO Results (ResultID, accuracy, f1_score, hyperparameter_config, PIN, ModelID, version_no, DatasetID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(Result result: results){
                try{
                    statement.setInt(1, result.getResultID());
                    statement.setDouble(2, result.getAccuracy());
                    statement.setDouble(3, result.getF1_score());
                    statement.setString(4, result.getHyperparameter_config());
                    statement.setInt(5, result.getPIN());
                    statement.setInt(6, result.getModelID());
                    statement.setString(7, result.getVersion_no());
                    statement.setInt(8, result.getDatasetID());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert Result: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in Results"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }

    //6.2 Task 2: Insert includes
    @Override
    public int insertincludes(include[] includes) {
        int rowsInserted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
// for database connection
        try {
            String query = "INSERT INTO includes (PubID, ResultID, placement_type, placement_section) VALUES (?, ?, ?, ?)" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            for(include include: includes){
                try{
                    statement.setInt(1, include.getPubID());
                    statement.setInt(2, include.getResultID());
                    statement.setString(3, include.getPlacement_type());
                    statement.setString(4, include.getPlacement_section());
                    statement.executeUpdate();
                    rowsInserted++;
                }
                catch (Exception e) {
//                    System.out.println("Task2 -> Failed to insert include: )" + e.getMessage());
                }
            }
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task2 -> Failed to connect to database in includes"  + ": " + e.getMessage());
        }
        return rowsInserted;
    }


    //6.3 Task 3: Find Users Without Profiles
    @Override
    public User[] getUsersWithoutProfiles() {
        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<User> users_without_profiles = new ArrayList<User>();
        try{
            String query =
                    "SELECT * \n" +
                    "FROM Users \n" +
                    "WHERE NOT EXISTS (SELECT * \n" +
                    "                  FROM Profiles \n" +
                    "                  WHERE Users.PIN = Profiles.PIN) \n"+
                    "ORDER BY reputation_score DESC, PIN ASC" ;
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            while(resultSet.next()){
                int user_PIN = resultSet.getInt("PIN");
                String user_name = resultSet.getString("user_name");
                int user_reputation = resultSet.getInt("reputation_score");
                //System.out.println("found -> " + user_PIN + " " + user_name + " " + user_reputation);
                User user  = new User(user_PIN, user_name, user_reputation);
                users_without_profiles.add(user);
//                System.out.println("Task3 This user has no profile -> " + resultSet.getString("user_name"));
            }
            resultSet.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task3 -> Failed to get users without profiles"  + ": " + e.getMessage());
        }


        return users_without_profiles.toArray(new User[0]);
    }


    //6.4 Task 4: Decrease Reputation for Users Without Profiles
    @Override
    public int decreaseReputationForMissingProfiles() {
        int rowsUpdated = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/

        try {
            String query_to_update =
                    "UPDATE Users \n" +
                    "SET Users.reputation_score = Users.reputation_score - 10  \n" +
                    "WHERE NOT EXISTS (SELECT * \n" +
                    "                  FROM Profiles \n" +
                    "                  WHERE Users.PIN = Profiles.PIN) and Users.reputation_score >=10";
            Statement statement = this.connection.createStatement();
            rowsUpdated = statement.executeUpdate(query_to_update);
//            System.out.println("Task4 affected rows calculated -> " + rowsUpdated);
            statement.close();
        }
        catch (SQLException e){
//                System.out.println("Task4 -> Failed to update or count updated users.reputation_score without profiles"  + ": " + e.getMessage());
        }

        return rowsUpdated;
    }


    //6.5 Task 5: Find Users With Specific Bio Keywords
    @Override
    public QueryResult.UserPINNameReputationBio[] getUsersByBioKeywords() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<QueryResult.UserPINNameReputationBio> results = new ArrayList<>();
        try {
            String query =
                    "SELECT Users.PIN, Users.user_name, Users.reputation_score, Profiles.bio " +
                            "FROM Users, Profiles " +
                            "WHERE Users.PIN = Profiles.PIN " +
                            "AND (LOWER(Profiles.bio) LIKE '%engineer%' " +
                            "     OR LOWER(Profiles.bio) LIKE '%scientist%' " +
                            "     OR LOWER(Profiles.bio) LIKE '%student%') " +
                            "ORDER BY Users.PIN ASC" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                 int PIN = result_row.getInt("PIN");
                 String user_name = result_row.getString("user_name");
                 int reputation_score = result_row.getInt("reputation_score");
                 String bio = result_row.getString("bio");
                 QueryResult.UserPINNameReputationBio row = new QueryResult.UserPINNameReputationBio(PIN,user_name,reputation_score,bio);
                 results.add(row);
//                 System.out.println("Task5 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task5 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.UserPINNameReputationBio[0]);
    }


    //6.6 Task 6: Find Organizations With No Released Models and Low Rating
    @Override
    public Organization[] getOrganizationsWithNoReleasedModelsAndLowRating() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<Organization> results = new ArrayList<>();
        try{
            String query =
                    "SELECT * \n" +
                    "FROM Organizations\n" +
                    "WHERE Organizations.rating < 2.5 and NOT EXISTS (SELECT *\n" +
                    "                                                 FROM Models\n" +
                    "                                                 WHERE Organizations.OrgID = Models.OrgID) \n" +
                    "ORDER BY Organizations.OrgID desc" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int orgID = result_row.getInt("OrgID");
                String org_name = result_row.getString("org_name");
                double rating = result_row.getDouble("rating");
                Organization row = new Organization(orgID,org_name,rating);
                results.add(row);
//                System.out.println("Task6 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();

        }
        catch (SQLException e){
//            System.out.println("Task6 -> Failed to get correct rows"  + ": " + e.getMessage());

        }
        return results.toArray(new Organization[0]);
    }


    // 6.7 Task 7: Delete Organizations With No Released Models and Low Rating
    @Override
    public int deleteOrganizationsWithNoReleasedModelsAndLowRating() {
        int rowsDeleted = 0;

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        try {
            String query_to_delete =
                    "DELETE\n" +
                    "FROM Organizations\n" +
                    "WHERE Organizations.rating < 2.5 and NOT EXISTS (SELECT *\n" +
                    "                                                 FROM Models\n" +
                    "                                                 WHERE Organizations.OrgID = Models.OrgID)" ;

            Statement statement = this.connection.createStatement();
            rowsDeleted = statement.executeUpdate(query_to_delete);
//            System.out.println("Task7 deleted rows calculated -> " + rowsDeleted);
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task7 -> Failed to delete or count deleted OrganizationsWithNoReleasedModelsAndLowRating"  + ": " + e.getMessage());
        }
        return rowsDeleted;
    }


    // 6.8 Task 8: Retrieve Models and Their Primary Task Information
    @Override
    public QueryResult.ModelPrimaryTaskInfo[] getModelPrimaryTaskInfo() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<QueryResult.ModelPrimaryTaskInfo> results = new ArrayList<>();
        try {
            String query =
                   "SELECT Models.ModelID, Models.model_name, Tasks.task_name AS primary_task_name, model_count.mcount as primary_task_count\n" +
                   "FROM Models, Tasks, designed_for, (SELECT designed_for.ModelID, COUNT(*) AS mcount\n" +
                   "                                   FROM designed_for\n" +
                   "                                   WHERE designed_for.is_primary = TRUE\n" +
                   "                                   GROUP BY designed_for.ModelID) AS model_count\n" +
                   "WHERE Models.ModelID = model_count.ModelID and designed_for.ModelID = Models.ModelID and designed_for.TaskID = Tasks.TaskID and designed_for.is_primary = TRUE\n" +
                   "ORDER BY Models.ModelID ASC" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int ModelID = result_row.getInt("ModelID");
                String model_name = result_row.getString("model_name");
                String primary_task_name = result_row.getString("primary_task_name");
                int primary_task_count = result_row.getInt("primary_task_count");
                QueryResult.ModelPrimaryTaskInfo row = new QueryResult.ModelPrimaryTaskInfo(ModelID,model_name,primary_task_name,primary_task_count);
                results.add(row);
//                System.out.println("Task8 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task8 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.ModelPrimaryTaskInfo[0]);
    }


    // 6.9 Task 9: Compute User Popularity Score
    @Override
    public QueryResult.UserPopularityInfo[] getUserPopularityScore() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<QueryResult.UserPopularityInfo> results = new ArrayList<>();
        try {
            String query =
                    "SELECT Users.PIN,\n" +
                    "    Users.user_name,\n" +
                    "    (COALESCE(temp_follower.follower_count, 0) - COALESCE(temp_followee.follows_count, 0)) AS popularity_score\n" +
                    "FROM Users\n" +
                    "LEFT JOIN (SELECT follows.followerPIN AS follower, COUNT(*) AS follows_count\n" +
                    "           FROM follows\n" +
                    "           GROUP BY follower) AS temp_followee ON Users.PIN = temp_followee.follower\n" +
                    "LEFT JOIN (SELECT follows.followeePIN AS account, COUNT(*) AS follower_count\n" +
                    "           FROM follows\n" +
                    "           GROUP BY account) AS temp_follower ON Users.PIN = temp_follower.account \n" +
                    "ORDER BY popularity_score DESC, Users.PIN ASC\n" +
                    "LIMIT 20" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int PIN = result_row.getInt("PIN");
                String user_name = result_row.getString("user_name");
                int popularity_score = result_row.getInt("popularity_score");
                QueryResult.UserPopularityInfo row = new QueryResult.UserPopularityInfo(PIN,user_name,popularity_score);
                results.add(row);
//                System.out.println("Task9 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task9 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.UserPopularityInfo[0]);
    }


    // 6.10 Task 10: Comprehensive Model Information
    @Override
    public QueryResult.ComprehensiveModelInfo[] getComprehensiveModelInfo() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<QueryResult.ComprehensiveModelInfo> results = new ArrayList<>();
        try {
            String query =
                    "SELECT Models.ModelID,  Models.model_name, Organizations.org_name, Models.license, Models.size, Primar.primary_task_name, Count.total_number_of_versions, Latest.latest_version_no, Latest.latest_version_date\n" +
                            "FROM Models, Organizations, (SELECT ModelID, task_name AS primary_task_name\n" +
                            "                                                   FROM designed_for, Tasks\n" +
                            "                                                   WHERE designed_for.is_primary = TRUE AND designed_for.TaskID = Tasks.TaskID) AS Primar, (SELECT ModelID, COUNT(*) total_number_of_versions\n" +
                            "                                                                                                                                                                                                                 FROM ModelVersions\n" +
                            "                                                                                                                                                                                                                 GROUP BY ModelID) as Count, (SELECT ModelVersions.ModelID, ModelVersions.version_no AS latest_version_no, ModelVersions.version_date AS latest_version_date\n" +
                            "                                                                                                                                                                                                                                                                      FROM ModelVersions, (SELECT ModelID, MAX(version_date) AS                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           version_date\n" +
                            "                                                                                                                                                                                                                                                                                                            FROM ModelVersions\n" +
                            "                                                                                                                                                                                                                                                                                                            GROUP BY ModelID) AS temp\n" +
                            "                                                                                                                                                                                                                                                                       WHERE ModelVersions.ModelID = temp.ModelID AND ModelVersions.version_date = temp.version_date) AS Latest\n" +
                            "WHERE Models.OrgID = Organizations.OrgID AND Models.ModelID = Primar.ModelID AND Models.ModelID = Count.ModelID AND Models.ModelID = Latest.ModelID\n" +
                            "ORDER BY Models.ModelID ASC" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int ModelID = result_row.getInt("ModelID");
                String model_name = result_row.getString("model_name");
                String org_name = result_row.getString("org_name");
                String license = result_row.getString("license");
                String size = result_row.getString("size");
                String primary_task_name = result_row.getString("primary_task_name");
                int total_number_of_versions = result_row.getInt("total_number_of_versions");
                String latest_version_no = result_row.getString("latest_version_no");
                String latest_version_date = result_row.getString("latest_version_date");

                QueryResult.ComprehensiveModelInfo row = new QueryResult.ComprehensiveModelInfo(ModelID,model_name, org_name, license, size, primary_task_name,total_number_of_versions, latest_version_no, latest_version_date);
                results.add(row);
//                System.out.println("Task10 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task10 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.ComprehensiveModelInfo[0]);
    }


    // 6.11 Task 11: Dataset Statistics by Modality
    @Override
    public QueryResult.DatasetStatisticsByModality[] getDatasetStatisticsByModality() {


        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/

        List<QueryResult.DatasetStatisticsByModality> results = new ArrayList<>();
        try {
            String query =
                    "SELECT modality, COUNT(*) AS dataset_count, AVG(number_of_rows) AS average_rows\n" +
                            "FROM Datasets\n" +
                            "GROUP BY modality\n" +
                            "ORDER BY average_rows DESC, modality ASC" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                String modality = result_row.getString("modality");
                int dataset_count = result_row.getInt("dataset_count");
                double average_rows = result_row.getDouble("average_rows");

                QueryResult.DatasetStatisticsByModality row = new QueryResult.DatasetStatisticsByModality(modality, dataset_count, average_rows);
                results.add(row);
//                System.out.println("Task11 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task11 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.DatasetStatisticsByModality[0]);
    }


    // 6.12 Task 12: Retrieve Large-Parameter Model Versions Within a Date Range
    @Override
    public QueryResult.LargeModelVersionInfo[] getLargeModelVersionsByDateRange(String start_date, String end_date) {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        // do we need to consider lik 0.0B etc
        List<QueryResult.LargeModelVersionInfo> results = new ArrayList<>();
        try {
            String query =
                    "SELECT Models.ModelID, Models.model_name, Models.size, ModelVersions.version_no, ModelVersions.version_date\n" +
                            "FROM Models, ModelVersions\n" +
                            "WHERE Models.size LIKE '%B' AND Models.ModelID = ModelVersions.ModelID AND ModelVersions.version_date >=  ? AND ModelVersions.version_date <= ? \n" +
                            "ORDER BY Models.ModelID ASC, version_date ASC" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            //insert inputs to query
            statement.setDate(1, Date.valueOf(start_date));
            statement.setDate(2, Date.valueOf(end_date));
            ResultSet result_row = statement.executeQuery();
            while(result_row.next()){
                int ModelID = result_row.getInt("ModelID");
                String model_name = result_row.getString("model_name");
                String size = result_row.getString("size");
                String version_no = result_row.getString("version_no");
                String version_date = result_row.getString("version_date");

                QueryResult.LargeModelVersionInfo row = new QueryResult.LargeModelVersionInfo(ModelID, model_name, size, version_no, version_date);
                results.add(row);
//                System.out.println("Task12 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task12 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.LargeModelVersionInfo[0]);
    }


    // 6.13 Task 13: Find Dataset(s) With Maximum Upload Count
    @Override
    public QueryResult.DatasetMaxUploadInfo[] getDatasetsWithMaxUploadCount() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        // look later not sure what they meant in question
        List<QueryResult.DatasetMaxUploadInfo> results = new ArrayList<>();
        try {
            String query =
                    "SELECT Datasets.DatasetID, Datasets.dataset_name, Count.upload_count " +
                            "FROM Datasets, " +
                            "     (SELECT DatasetID, COUNT(*) AS upload_count " +
                            "      FROM uploads " +
                            "      GROUP BY DatasetID " +
                            "      HAVING COUNT(*) >= ALL (SELECT COUNT(*) " +
                            "                              FROM uploads AS u " +
                            "                              GROUP BY u.DatasetID)) AS Count " +
                            "WHERE Datasets.DatasetID = Count.DatasetID " +
                            "ORDER BY Datasets.DatasetID ASC" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int DatasetID = result_row.getInt("DatasetID");
                String dataset_name = result_row.getString("dataset_name");
                int upload_count = result_row.getInt("upload_count");

                QueryResult.DatasetMaxUploadInfo row = new QueryResult.DatasetMaxUploadInfo(DatasetID, dataset_name, upload_count);
                results.add(row);
//                System.out.println("Task13 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task13 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.DatasetMaxUploadInfo[0]);
    }


    // 6.14 Task 14: Find Complete Dataset(s) with All Roles
    @Override
    public Dataset[] getCompleteDatasets() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<Dataset> results = new ArrayList<>();
        try{
            String query =
                    "SELECT D.DatasetID, D.dataset_name, D.modality, D.number_of_rows\n" +
                            "FROM Datasets D\n" +
                            "JOIN uploads U ON D.DatasetID = U.DatasetID\n" +
                            "GROUP BY D.DatasetID, D.dataset_name, D.modality, D.number_of_rows\n" +
                            "HAVING COUNT(DISTINCT U.role) = (\n" +
                            "    SELECT COUNT(DISTINCT role) \n" +
                            "    FROM uploads\n" +
                            ")\n" +
                            "ORDER BY D.DatasetID ASC" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int DatasetID = result_row.getInt("DatasetID");
                String dataset_name = result_row.getString("dataset_name");
                String modality = result_row.getString("modality");
                int number_of_rows = result_row.getInt("number_of_rows");
                Dataset row = new Dataset(DatasetID,dataset_name, modality,number_of_rows);
                results.add(row);
//                System.out.println("Task14 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();

        }
        catch (SQLException e){
//            System.out.println("Task14 -> Failed to get correct rows"  + ": " + e.getMessage());

        }
        return results.toArray(new Dataset[0]);
    }


    // 6.15 Task 15: Find Users Who Uploaded Datasets with Role 'creator' or 'contributor' but Never 'validator' and Have Reputation ≥ 60
    @Override
    public User[] getUsersCreatorOrContributorButNotValidator() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<User> results = new ArrayList<>();
        try{
            String query =
                    "SELECT *\n" +
                            "FROM Users \n" +
                            "WHERE reputation_score >= 60 AND EXISTS (SELECT *\n" +
                            "                                                                            FROM uploads\n" +
                            "                                                                             WHERE uploads.PIN = Users.PIN AND (role = 'creator' OR role = 'contributor')) AND NOT EXISTS  (SELECT *\n" +
                            "                                                                                                                                                                                                                                             FROM uploads\n" +
                            "                                                                                                                                                                                                                                             WHERE uploads.PIN = Users.PIN AND role = 'validator')\n" +
                            "ORDER BY PIN ASC" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int PIN = result_row.getInt("PIN");
                String user_name = result_row.getString("user_name");
                int reputation_score = result_row.getInt("reputation_score");
                User row = new User(PIN, user_name, reputation_score);
                results.add(row);
//                System.out.println("Task15 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();

        }
        catch (SQLException e){
//            System.out.println("Task15 -> Failed to get correct rows"  + ": " + e.getMessage());

        }
        return results.toArray(new User[0]);
    }


    // 6.16 Task 16: Find Users Who Ran All Versions of at Least One Model
    @Override
    public QueryResult.UserModelVersionInfo[] getUsersWhoRanAllVersionsOfModels() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        //SELECT PIN, user_name, UsersWModels_ran_all_versions.ModelID, model_name, version_no, license
        //FROM ModelVersions, (SELECT *
        //FROM Users, Models
        //WHERE Models.ModelID IN (SELECT M.ModelID
        //                                                                                                                                        FROM Models M
        //                                                                                                                                         WHERE NOT EXISTS(SELECT *
        //                                                                                                                                                                              FROM ModelVersions Mv
        //                                                                                                                                                                              WHERE M.ModelID = Mv.ModelID AND NOT EXISTS (SELECT *
        //                                                                                                                                                                                                                                                                      FROM runs r
        //                                                                                                                                                                                                                                                                      WHERE r.PIN = Users.PIN AND r.ModelID = M.ModelID AND r.version_no = Mv.version_no)))) UsersWModels_ran_all_versions
        //WHERE  UsersWModels_ran_all_versions.ModelID = ModelVersions.ModelID
        //ORDER BY PIN, UsersWModels_ran_all_versions.ModelID, version_no ASC;
        // if current one is wrong this is the basic but still complex logic
        List<QueryResult.UserModelVersionInfo> results = new ArrayList<>();
        try {
            String query =
                    "SELECT \n" +
                            "    U.PIN, \n" +
                            "    U.user_name, \n" +
                            "    M.ModelID, \n" +
                            "    M.model_name, \n" +
                            "    MV.version_no, \n" +
                            "    M.license\n" +
                            "FROM \n" +
                            "    Users U, \n" +
                            "    Models M, \n" +
                            "    ModelVersions MV\n" +
                            "WHERE \n" +
                            "    M.ModelID = MV.ModelID  \n" +
                            "    AND NOT EXISTS (\n" +
                            "        SELECT 1\n" +
                            "        FROM ModelVersions MV_Check\n" +
                            "        WHERE MV_Check.ModelID = M.ModelID\n" +
                            "        AND NOT EXISTS (\n" +
                            "            SELECT 1\n" +
                            "            FROM runs R\n" +
                            "            WHERE R.PIN = U.PIN \n" +
                            "            AND R.ModelID = M.ModelID \n" +
                            "            AND R.version_no = MV_Check.version_no\n" +
                            "        )\n" +
                            "    )\n" +
                            "ORDER BY \n" +
                            "    U.PIN ASC, \n" +
                            "    M.ModelID ASC, \n" +
                            "    MV.version_no ASC" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int PIN = result_row.getInt("PIN");
                String user_name = result_row.getString("user_name");
                int ModelID = result_row.getInt("ModelID");
                String model_name = result_row.getString("model_name");
                String version_no = result_row.getString("version_no");
                String license = result_row.getString("license");

                QueryResult.UserModelVersionInfo row = new QueryResult.UserModelVersionInfo(PIN, user_name, ModelID, model_name, version_no, license);
                results.add(row);
//                System.out.println("Task16 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task16 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.UserModelVersionInfo[0]);
    }


    // 6.17 Task 17: Run-Type Statistics
    @Override
    public QueryResult.RunTypeStats[] getRunTypeStatistics() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<QueryResult.RunTypeStats> results = new ArrayList<>();
        try {
            String query =
                    "SELECT run_type, COUNT(*) AS total_number_of_results, AVG(f1_score) AS average_f1_score\n" +
                            "FROM runs, Results\n" +
                            "WHERE Results.PIN = runs.PIN AND Results.ModelID = runs.ModelID AND Results.version_no = runs.version_no AND Results.DatasetID = runs.DatasetID \n" +
                            "GROUP BY run_type\n" +
                            "ORDER BY run_type DESC" ;
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                String run_type = result_row.getString("run_type");
                int total_number_of_results = result_row.getInt("total_number_of_results");
                double average_f1_score = result_row.getDouble("average_f1_score");

                QueryResult.RunTypeStats row = new QueryResult.RunTypeStats(run_type, total_number_of_results, average_f1_score);
                results.add(row);
//                System.out.println("Task17 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task17 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.RunTypeStats[0]);
    }


    // 6.18 Task 18: Find Publications That Include Results From Runs of a Dataset
    @Override
    public Publication[] getPublicationsUsingDataset(String dataset_name) {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        // ı used distinct but not sure if they asked or wanted that just to eliminate inserting same object but def check
        List<Publication> results = new ArrayList<>();
        try{
            String query =
                    "SELECT DISTINCT Publications.PubID, title, venue\n" +
                            "FROM Publications, includes, Results, runs, Datasets\n" +
                            "WHERE Datasets.dataset_name = ? AND  runs.DatasetID = Datasets.DatasetID AND Results.PIN = runs.PIN AND Results.ModelID = runs.ModelID AND Results.version_no = runs.version_no AND Results.DatasetID = runs.DatasetID AND includes.ResultID = Results.ResultID AND Publications.PubID = includes.PubID AND Results.accuracy >= 0.70\n" +
                            "ORDER BY Publications.PubID ASC" ;
            PreparedStatement statement = this.connection.prepareStatement(query);
            statement.setString(1, dataset_name);
            ResultSet result_row = statement.executeQuery();
            while(result_row.next()){
                int PubID = result_row.getInt("PubID");
                String title = result_row.getString("title");
                String venue = result_row.getString("venue");
                Publication row = new Publication(PubID, title, venue);
                results.add(row);
//                System.out.println("Task18 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();

        }
        catch (SQLException e){
//            System.out.println("Task18 -> Failed to get correct rows"  + ": " + e.getMessage());

        }
        return results.toArray(new Publication[0]);
    }


    // 6.19 Task 19: Find Top 10 Highly-Reputed Users
    @Override
    public QueryResult.HighlyReputedUser[] getTopTenHighlyReputedUsers() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<QueryResult.HighlyReputedUser> results = new ArrayList<>();
        try {
            String query =
                    "SELECT Users.PIN, Users.user_name, (COALESCE(Publication_count.number_of_publications_including_user_result, 0) + COALESCE(Owner_count.number_of_owner_uploads, 0) + COALESCE( Users.reputation_score, 0)) AS user_score\n" +
                            "FROM Users\n" +
                            "LEFT JOIN (SELECT PIN, COUNT(*) AS number_of_owner_uploads\n" +
                            "                        FROM uploads \n" +
                            "                        WHERE role = 'owner' \n" +
                            "                        GROUP BY PIN) AS  Owner_count ON Users.PIN = Owner_count.PIN\n" +
                            "LEFT JOIN  (SELECT Results.PIN, COUNT(DISTINCT includes.PubID) AS number_of_publications_including_user_result\n" +
                            "                                                                                                  FROM includes, Results                                                                                                  \n" +
                            "                                                                                                  WHERE includes.ResultID = Results.ResultID\n" +
                            "                                                                                                  GROUP BY Results.PIN) AS Publication_count ON Users.PIN = Publication_count.PIN\n" +
                            "\n" +
                            "ORDER BY user_score DESC, Users.PIN ASC\n" +
                            "LIMIT 10";
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int  PIN = result_row.getInt("PIN");
                String user_name = result_row.getString("user_name");
                int user_score = result_row.getInt("user_score");

                QueryResult.HighlyReputedUser row = new QueryResult.HighlyReputedUser(PIN, user_name, user_score);
                results.add(row);
//                System.out.println("Task19 row added to result -> " + row.toString());
            }
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task19 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.HighlyReputedUser[0]);
    }


    // 6.20 Task 20: Find Vulnerability Detection Publications
    @Override
    public QueryResult.TaskSpecificPublication[] getVulnerabilityDetectionPublications() {

        /*****************************************************/
        /*****************************************************/
        /*********************  TODO  ***********************/
        /*****************************************************/
        /*****************************************************/
        List<QueryResult.TaskSpecificPublication> results = new ArrayList<>();
        try {
            String query =
                    "SELECT P.PubID, R.ResultID, P.title, P.venue, runs.run_type, R.f1_score, R.accuracy, R.hyperparameter_config, I.placement_type, I.placement_section, U.user_name, M.model_name, M.size, R.version_no, D.dataset_name\n" +
                            "FROM Publications P, includes I, Results R, runs , Users U, Models M, designed_for DF, Tasks T, Datasets D\n" +
                            "WHERE P.PubID = I.PubID AND I.ResultID = R.ResultID AND R.PIN = runs.PIN AND R.ModelID = runs.ModelID AND R.version_no = runs.version_no AND R.DatasetID = runs.DatasetID AND runs.PIN = U.PIN AND \n" +
                            "               runs.ModelID = M.ModelID AND runs.DatasetID = D.DatasetID AND M.ModelID = DF.ModelID AND DF.TaskID = T.TaskID AND T.task_name = 'Vulnerability Detection' AND R.accuracy >= 0.70 AND R.f1_score >= 0.70\n" +
                            "ORDER BY P.PubID ASC, R.ResultID ASC";
            Statement statement = this.connection.createStatement();
            ResultSet result_row = statement.executeQuery(query);
            while(result_row.next()){
                int  pubID = result_row.getInt("pubID");
                int  resultID = result_row.getInt("resultID");
                String title = result_row.getString("title");
                String venue = result_row.getString("venue");
                String run_type = result_row.getString("run_type");
                double f1_score = result_row.getDouble("f1_score");
                double accuracy = result_row.getDouble("accuracy");
                String hyperparameter_config = result_row.getString("hyperparameter_config");
                String placement_type = result_row.getString("placement_type");
                String placement_section = result_row.getString("placement_section");
                String user_name = result_row.getString("user_name");
                String model_name = result_row.getString("model_name");
                String size = result_row.getString("size");
                String version_no = result_row.getString("version_no");
                String dataset_name = result_row.getString("dataset_name");

                QueryResult.TaskSpecificPublication row = new QueryResult.TaskSpecificPublication(pubID, resultID, title, venue, run_type, f1_score, accuracy, hyperparameter_config, placement_type, placement_section, user_name, model_name, size, version_no, dataset_name);
                results.add(row);
//                System.out.println("Task20 row added to result -> " + row.toString());
            }
//            System.out.println("CONGRATS FINISHED ALL 20 TASK <- ╾━╤デ╦︻ (•_- ) ‍️‍" );
            result_row.close();
            statement.close();
        }
        catch (SQLException e){
//            System.out.println("Task20 -> Failed to get correct rows"  + ": " + e.getMessage());
        }
        return results.toArray(new QueryResult.TaskSpecificPublication[0]);
    }


}