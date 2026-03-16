package ceng.ceng351.ModelHubPlatform;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.io.File;

import org.h2.tools.Server;

/*
  ================== EVALUATION CLASS – QUICK EXPLANATION ==================

  • This program creates an in-memory H2 database:
        dbc:h2:mem:ModelHubPlatformdb;DB_CLOSE_DELAY=-1"
        This program uses an H2 in-memory database.
        The database is deleted when the JVM fully stops.
        However, if the IDE keeps the JVM alive between runs,
        the previous tables may remain, which is why dropTables() removes leftover tables at the start.

  • It starts:
        – H2 TCP Server (port 9092)
        – H2 Web Console (http://localhost:8082)

  • To inspect your database:
        → Open your browser and enter: http://localhost:8082
        → Or simply click the link shown in the console output.
        → Use this JDBC URL in the login page:
              jdbc:h2:tcp://localhost/mem:ModelHubPlatformdb
          User: sa   Password: (empty)

  • Before creating tables, dropTables() removes tables left from previous runs.

  • At the end, the program waits:
        "Press Enter to Quit..."
    Use this time to connect to the Web Console and explore your database.

  • IMPORTANT:
        After you press Enter, the program shuts down:
            – Web Console stops
            – TCP server stops
            – Database is destroyed
        So the localhost page will no longer connect afterward.

  ========================================================================
*/

public class Evaluation {

    private static final String user = "sa";
    private static final String password = "";
    private static Connection connection = null;

    // IN-MEMORY H2 DATABASE
    private static final String url = "jdbc:h2:mem:ModelHubPlatformdb;DB_CLOSE_DELAY=-1";

    private static Server tcpServer;
    private static Server webServer;

    // -----------------------------
    // CONNECT
    // -----------------------------
    public static void connect() {
        try {
            connection = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to H2 in-memory DB.");

            // Start TCP server (allows external connections like H2 Console)
            tcpServer = Server.createTcpServer("-tcp", "-tcpAllowOthers", "-tcpPort", "9092").start();

            // Start H2 Web Console
            webServer = Server.createWebServer("-web", "-webAllowOthers", "-webPort", "8082").start();

            System.out.println("H2 Console: http://localhost:8082");
            System.out.println("JDBC URL for console: jdbc:h2:tcp://localhost/mem:ModelHubPlatformdb");

        } catch (SQLException e) {
            System.out.println("Cannot connect to database!");
            e.printStackTrace();
        }
    }

    // -----------------------------
    // DISCONNECT
    // -----------------------------
    public static void disconnect() {
        try {
            if (connection != null) {
                connection.close();
                System.out.println("Disconnected from H2 memory DB.");
            }

            if (webServer != null) {
                webServer.stop();
                System.out.println("H2 Web Console stopped.");
            }

            if (tcpServer != null) {
                tcpServer.stop();
                System.out.println("H2 TCP Server stopped.");
            }

        } catch (SQLException e) {
            System.out.println("Error while disconnecting!");
            e.printStackTrace();
        }
    }

    // Writers
    public static void addInputTitle(String title, BufferedWriter bw) throws IOException {
        bw.write("***** " + title + " *****\n");
    }

    public static void printLine(String result, BufferedWriter bw) throws IOException {
        bw.write(result + "\n");
    }

    public static void addDivider(BufferedWriter bw) throws IOException {
        bw.write("\n--------------------------------------------------------------\n");
    }

    // -----------------------------
    //  DROP TABLES
    // -----------------------------
    public static void dropExistingTables(ModelHubPlatform system) {
        try {
            int dropped = system.dropTables();   // number of dropped tables
            System.out.println("---------------------------");
            System.out.println("Dropped " + dropped + " tables from previous run.");
            System.out.println("---------------------------");
        } catch (Exception e) {
            System.out.println("Error dropping tables: " + e);
        }
    }


    // -----------------------------
    // MAIN
    // -----------------------------
    public static void main(String[] args) {

        int numberOfInsertions = 0;
        int numberOfTablesCreated = 0;

        BufferedWriter bufferedWriter = null;

        // CONNECT
        connect();

        // ModelHubPlatform initialize
        ModelHubPlatform ModelHubPlatform = new ModelHubPlatform();
        ModelHubPlatform.initialize(connection);

        try {

            // OUTPUT DIR
            String outputDirectory = System.getProperty("user.dir") + File.separator + "output";
            new File(outputDirectory).mkdir();

            bufferedWriter = new BufferedWriter(new FileWriter(outputDirectory + File.separator + "Output.txt"));

            // --- OUTPUT FILE START TITLE ---
            printLine("========== STARTING OUTPUT FILE ==========", bufferedWriter);


            // -----------------------------
            //  DROP TABLES IF THERE EXISTS TABLES FROM PREVIOUS RUN
            // -----------------------------
            dropExistingTables(ModelHubPlatform);


            // -----------------------------
            //  CREATE TABLES
            // -----------------------------

            //6.1 Task 1: Create Database Tables
            addDivider(bufferedWriter);
            addInputTitle("Task 1: Create Database Tables", bufferedWriter);
            try {
                numberOfTablesCreated = ModelHubPlatform.createTables();
                printLine("Created " + numberOfTablesCreated + " tables.", bufferedWriter);
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during createTables(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // -----------------------------
            //  INSERT DATA INTO TABLES
            //  6.2 Task 2: Insert Data into Tables
            // -----------------------------

            //6.2 Task 2: Insert Users
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert Users", bufferedWriter);
            numberOfInsertions = 0;
            User[] users = FileOperations.readUsersFile();
            numberOfInsertions = ModelHubPlatform.insertUsers(users);
            printLine(numberOfInsertions + " users were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert Organizations
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert Organizations", bufferedWriter);
            numberOfInsertions = 0;
            Organization[] organizations = FileOperations.readOrganizationsFile();
            numberOfInsertions = ModelHubPlatform.insertOrganizations(organizations);
            printLine(numberOfInsertions + " organizations were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert Tasks
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert Tasks", bufferedWriter);
            numberOfInsertions = 0;
            Task[] tasks = FileOperations.readTasksFile();
            numberOfInsertions = ModelHubPlatform.insertTasks(tasks);
            printLine(numberOfInsertions + " tasks were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert Datasets
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert Datasets", bufferedWriter);
            numberOfInsertions = 0;
            Dataset[] datasets = FileOperations.readDatasetsFile();
            numberOfInsertions = ModelHubPlatform.insertDatasets(datasets);
            printLine(numberOfInsertions + " datasets were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert Publications
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert Publications", bufferedWriter);
            numberOfInsertions = 0;
            Publication[] publications = FileOperations.readPublicationsFile();
            numberOfInsertions = ModelHubPlatform.insertPublications(publications);
            printLine(numberOfInsertions + " publications were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert Profiles
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert Profiles", bufferedWriter);
            numberOfInsertions = 0;
            Profile[] profiles = FileOperations.readProfilesFile();
            numberOfInsertions = ModelHubPlatform.insertProfiles(profiles);
            printLine(numberOfInsertions + " profiles were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert follows
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert follows", bufferedWriter);
            numberOfInsertions = 0;
            follow[] follows = FileOperations.readfollowsFile();
            numberOfInsertions = ModelHubPlatform.insertfollows(follows);
            printLine(numberOfInsertions + " follows were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert Models
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert Models", bufferedWriter);
            numberOfInsertions = 0;
            Model[] models = FileOperations.readModelsFile();
            numberOfInsertions = ModelHubPlatform.insertModels(models);
            printLine(numberOfInsertions + " models were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert uploads
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert uploads", bufferedWriter);
            numberOfInsertions = 0;
            upload[] uploads = FileOperations.readuploadsFile();
            numberOfInsertions = ModelHubPlatform.insertuploads(uploads);
            printLine(numberOfInsertions + " uploads were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert ModelVersions
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert ModelVersions", bufferedWriter);
            numberOfInsertions = 0;
            ModelVersion[] modelVersions = FileOperations.readModelVersionsFile();
            numberOfInsertions = ModelHubPlatform.insertModelVersions(modelVersions);
            printLine(numberOfInsertions + " model versions were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert designed_for
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert designed_for", bufferedWriter);
            numberOfInsertions = 0;
            designed_for[] designedFors = FileOperations.readdesigned_forsFile();
            numberOfInsertions = ModelHubPlatform.insertdesigned_fors(designedFors);
            printLine(numberOfInsertions + " designed_for(s) were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert runs
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert runs", bufferedWriter);
            numberOfInsertions = 0;
            run[] runs = FileOperations.readrunsFile();
            numberOfInsertions = ModelHubPlatform.insertruns(runs);
            printLine(numberOfInsertions + " runs were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert Results
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert Results", bufferedWriter);
            numberOfInsertions = 0;
            Result[] results = FileOperations.readResultsFile();
            numberOfInsertions = ModelHubPlatform.insertResults(results);
            printLine(numberOfInsertions + " results were inserted.", bufferedWriter);
            addDivider(bufferedWriter);

            //6.2 Task 2: Insert includes
            addDivider(bufferedWriter);
            addInputTitle("Task 2: Insert includes", bufferedWriter);
            numberOfInsertions = 0;
            include[] includes = FileOperations.readincludesFile();
            numberOfInsertions = ModelHubPlatform.insertincludes(includes);
            printLine(numberOfInsertions + " includes were inserted.", bufferedWriter);
            addDivider(bufferedWriter);


            //6.3 Task 3: Find Users Without Profiles
            addDivider(bufferedWriter);
            addInputTitle("Task 3: Users Without Profiles", bufferedWriter);
            try {
                User[] usersWithoutProfiles = ModelHubPlatform.getUsersWithoutProfiles();
                if (usersWithoutProfiles != null && usersWithoutProfiles.length > 0) {
                    for (User user : usersWithoutProfiles) {
                        printLine(user.toString(), bufferedWriter);
                    }
                    printLine(usersWithoutProfiles.length + " users without profiles found.", bufferedWriter);
                } else {
                    printLine("No users without profiles found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during getUsersWithoutProfiles(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            //6.4 Task 4: Decrease Reputation for Users Without Profiles
            addDivider(bufferedWriter);
            addInputTitle("Task 4: Decrease Reputation for Users Without Profiles", bufferedWriter);
            try {
                int numberOfUpdates = ModelHubPlatform.decreaseReputationForMissingProfiles();
                if (numberOfUpdates > 0) {
                    printLine(numberOfUpdates + " user reputations were decreased.", bufferedWriter);
                } else {
                    printLine("No user reputations were decreased.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during decreaseReputationForMissingProfiles(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            //Users Without Profiles to Test 'Task 4: Decrease Reputation for Users Without Profiles'
            addDivider(bufferedWriter);
            addInputTitle("Users Without Profiles to Test 'Task 4: Decrease Reputation for Users Without Profiles'", bufferedWriter);
            try {
                User[] usersWithoutProfiles = ModelHubPlatform.getUsersWithoutProfiles();
                if (usersWithoutProfiles != null && usersWithoutProfiles.length > 0) {
                    for (User user : usersWithoutProfiles) {
                        printLine(user.toString(), bufferedWriter);
                    }
                    printLine(usersWithoutProfiles.length + " users without profiles found.", bufferedWriter);
                } else {
                    printLine("No users without profiles found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during getUsersWithoutProfiles(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            //6.5 Task 5: Find Users With Specific Bio Keywords
            addDivider(bufferedWriter);
            addInputTitle("Task 5: Users With Specific Bio Keywords", bufferedWriter);
            try {
                QueryResult.UserPINNameReputationBio[] usersWithBioKeywords = ModelHubPlatform.getUsersByBioKeywords();
                if (usersWithBioKeywords != null && usersWithBioKeywords.length > 0) {
                    for (QueryResult.UserPINNameReputationBio user : usersWithBioKeywords) {
                        printLine(user.toString(), bufferedWriter);
                    }
                    printLine(usersWithBioKeywords.length + " users with matching bio keywords found.", bufferedWriter);
                } else {
                    printLine("No users with matching bio keywords found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during getUsersByBioKeywords(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            //6.6 Task 6: Find Organizations With No Released Models and Low Rating
            addDivider(bufferedWriter);
            addInputTitle("Task 6: Organizations With No Released Models and Low Rating", bufferedWriter);
            try {
                Organization[] lowRatedOrgs = ModelHubPlatform.getOrganizationsWithNoReleasedModelsAndLowRating();
                if (lowRatedOrgs != null && lowRatedOrgs.length > 0) {
                    for (Organization org : lowRatedOrgs) {
                        printLine(org.toString(), bufferedWriter);
                    }
                    printLine(lowRatedOrgs.length + " organizations with no released models and low rating were found.", bufferedWriter);
                } else {
                    printLine("No organizations with no released models and low rating were found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during getOrganizationsWithNoReleasedModelsAndLowRating(): "
                        + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            //6.7 Task 7: Delete Organizations With No Released Models and Low Rating
            addDivider(bufferedWriter);
            addInputTitle("Task 7: Delete Organizations With No Released Models and Low Rating", bufferedWriter);
            try {
                int deletedCount = ModelHubPlatform.deleteOrganizationsWithNoReleasedModelsAndLowRating();
                printLine(deletedCount + " organizations with no released models and low rating were deleted.", bufferedWriter);
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during deleteOrganizationsWithNoReleasedModelsAndLowRating(): "
                        + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            //Organizations With No Released Models and Low Rating to Test 'Task 7: Delete Organizations With No Released Models and Low Rating'
            addDivider(bufferedWriter);
            addInputTitle("Organizations With No Released Models and Low Rating to Test 'Task 7: Delete Organizations With No Released Models and Low Rating'", bufferedWriter);
            try {
                Organization[] lowRatedOrgs = ModelHubPlatform.getOrganizationsWithNoReleasedModelsAndLowRating();
                if (lowRatedOrgs != null && lowRatedOrgs.length > 0) {
                    for (Organization org : lowRatedOrgs) {
                        printLine(org.toString(), bufferedWriter);
                    }
                    printLine(lowRatedOrgs.length + " organizations with no released models and low rating were found.", bufferedWriter);
                } else {
                    printLine("No organizations with no released models and low rating were found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during getOrganizationsWithNoReleasedModelsAndLowRating(): "
                        + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.8 Task 8: Retrieve Models and Their Primary Task Information
            addDivider(bufferedWriter);
            addInputTitle("Task 8: Models and Their Primary Task Information", bufferedWriter);
            try {
                QueryResult.ModelPrimaryTaskInfo[] infos = ModelHubPlatform.getModelPrimaryTaskInfo();
                if (infos != null && infos.length > 0) {
                    for (QueryResult.ModelPrimaryTaskInfo info : infos) {
                        printLine(info.toString(), bufferedWriter);
                    }
                    printLine(infos.length + " models retrieved with primary task information.", bufferedWriter);
                } else {
                    printLine("No model primary task information were found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during getModelPrimaryTaskInfo(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.9 Task 9: Compute User Popularity Score
            addDivider(bufferedWriter);
            addInputTitle("Task 9: User Popularity Score", bufferedWriter);
            try {
                QueryResult.UserPopularityInfo[] infos = ModelHubPlatform.getUserPopularityScore();
                if (infos != null && infos.length > 0) {
                    for (QueryResult.UserPopularityInfo info : infos) {
                        printLine(info.toString(), bufferedWriter);
                    }
                    printLine(infos.length + " users retrieved with popularity score.", bufferedWriter);
                } else {
                    printLine("No user popularity score found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during getUserPopularityScore(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.10 Task 10: Comprehensive Model Information
            addDivider(bufferedWriter);
            addInputTitle("Task 10: Comprehensive Model Information", bufferedWriter);
            try {
                QueryResult.ComprehensiveModelInfo[] infos = ModelHubPlatform.getComprehensiveModelInfo();
                if (infos != null && infos.length > 0) {
                    for (QueryResult.ComprehensiveModelInfo info : infos) {
                        printLine(info.toString(), bufferedWriter);
                    }
                    printLine(infos.length + " models retrieved with comprehensive information.", bufferedWriter);
                } else {
                    printLine("No comprehensive model information found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception during getComprehensiveModelInfo(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.11 Task 11: Dataset Statistics by Modality
            addDivider(bufferedWriter);
            addInputTitle("Task 11: Dataset Statistics by Modality", bufferedWriter);
            try {
                QueryResult.DatasetStatisticsByModality[] infos = ModelHubPlatform.getDatasetStatisticsByModality();
                if (infos != null && infos.length > 0) {
                    for (QueryResult.DatasetStatisticsByModality info : infos) {
                        printLine(info.toString(), bufferedWriter);
                    }
                    printLine(infos.length + " modality statistics retrieved.", bufferedWriter);
                } else {
                    printLine("No dataset modality statistics found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during getDatasetStatisticsByModality(): "
                        + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.12 Task 12: Retrieve Large-Parameter Model Versions Within a Date Range
            addDivider(bufferedWriter);
            String start_date = "2024-01-01";
            String end_date = "2025-12-03";
            addInputTitle("Task 12: Large-Parameter Model Versions Between " + start_date + " and " + end_date, bufferedWriter);
            try {
                QueryResult.LargeModelVersionInfo[] infos = ModelHubPlatform.getLargeModelVersionsByDateRange(start_date, end_date);
                if (infos != null && infos.length > 0) {
                    for (QueryResult.LargeModelVersionInfo info : infos) { printLine(info.toString(), bufferedWriter); }
                    printLine(infos.length + " large-parameter model versions retrieved between " + start_date + " and " + end_date + ".", bufferedWriter);
                } else {
                    printLine("No large-parameter model versions found between " + start_date + " and " + end_date + ".", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception during getLargeModelVersionsByDateRange(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.13 Task 13: Find Dataset(s) With Maximum Upload Count
            addDivider(bufferedWriter);
            addInputTitle("Task 13: Dataset(s) With Maximum Upload Count", bufferedWriter);
            try {
                QueryResult.DatasetMaxUploadInfo[] infos = ModelHubPlatform.getDatasetsWithMaxUploadCount();
                if (infos != null && infos.length > 0) {
                    for (QueryResult.DatasetMaxUploadInfo info : infos) {
                        printLine(info.toString(), bufferedWriter);
                    }
                    printLine(infos.length + " dataset(s) with maximum upload count found.", bufferedWriter);
                } else {
                    printLine("No dataset upload information found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception occurred during getDatasetsWithMaxUploadCount(): "
                        + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.14 Task 14: Find Complete Datasets with All Roles
            addDivider(bufferedWriter);
            addInputTitle("Task 14: Complete Datasets Uploaded by All Roles", bufferedWriter);
            try {
                Dataset[] infos = ModelHubPlatform.getCompleteDatasets();
                if (infos != null && infos.length > 0) {
                    for (Dataset info : infos) {
                        printLine(info.toString(), bufferedWriter);
                    }
                    printLine(infos.length + " complete datasets found.", bufferedWriter);
                } else {
                    printLine("No complete datasets found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception during getCompleteDatasets(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.15 Task 15: Find Users Who Uploaded Datasets with Role 'creator' or 'contributor' but Never 'validator' and Have Reputation ≥ 60
            addDivider(bufferedWriter);
            addInputTitle("Task 15: Users Who Uploaded Datasets with Role 'creator' or 'contributor' but Never 'validator' and Have Reputation ≥ 60", bufferedWriter);
            try {
                User[] infos = ModelHubPlatform.getUsersCreatorOrContributorButNotValidator();
                if (infos != null && infos.length > 0) {
                    for (User info : infos) {
                        printLine(info.toString(), bufferedWriter);
                    }
                    printLine(infos.length + " users retrieved.", bufferedWriter);
                } else {
                    printLine("No matching users found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception during getUsersCreatorOrContributorButNotValidator(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.16 Task 16: Find Users Who Ran All Versions of at Least One Model
            addDivider(bufferedWriter);
            addInputTitle("Task 16: Users Who Ran All Versions of At Least One Model", bufferedWriter);
            try {
                QueryResult.UserModelVersionInfo[] infos = ModelHubPlatform.getUsersWhoRanAllVersionsOfModels();
                if (infos != null && infos.length > 0) {
                    for (QueryResult.UserModelVersionInfo info : infos) {
                        printLine(info.toString(), bufferedWriter);
                    }
                    printLine(infos.length + " users found who ran all versions of any model.", bufferedWriter);
                } else {
                    printLine("No users found who ran all versions of any model.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception during getUsersWhoRanAllVersionsOfModels(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.17 Task 17: Run-Type Statistics
            addDivider(bufferedWriter);
            addInputTitle("Task 17: Run-Type Statistics", bufferedWriter);
            try {
                QueryResult.RunTypeStats[] stats = ModelHubPlatform.getRunTypeStatistics();
                if (stats != null && stats.length > 0) {
                    for (QueryResult.RunTypeStats s : stats) {
                        printLine(s.toString(), bufferedWriter);
                    }
                    printLine(stats.length + " run types found.", bufferedWriter);
                } else {
                    printLine("No run-type statistics found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception during getRunTypeStatistics(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.18 Task 18: Find Publications That Include Results From Runs of a Dataset
            addDivider(bufferedWriter);
            String dataset_name = "BigVul";
            addInputTitle("Task 18: Publications That Include Results From Runs of Dataset " + dataset_name, bufferedWriter);
            try {
                Publication[] pubs = ModelHubPlatform.getPublicationsUsingDataset(dataset_name);
                if (pubs != null && pubs.length > 0) {
                    for (Publication p : pubs) {
                        printLine(p.toString(), bufferedWriter);
                    }
                    printLine(pubs.length + " publications found for dataset " + dataset_name + ".", bufferedWriter);
                } else {
                    printLine("No publications found for dataset " + dataset_name + ".", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception during getPublicationsUsingDataset(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.19 Task 19: Find Top 10 Highly-Reputed Users
            addDivider(bufferedWriter);
            addInputTitle("Task 19: Top 10 Highly-Reputed Users", bufferedWriter);
            try {
                QueryResult.HighlyReputedUser[] infos = ModelHubPlatform.getTopTenHighlyReputedUsers();
                if (infos != null && infos.length > 0) {
                    for (QueryResult.HighlyReputedUser info : infos) {
                        printLine(info.toString(), bufferedWriter);
                    }
                    printLine(infos.length + " highly-reputed users found.", bufferedWriter);
                } else {
                    printLine("No highly-reputed users found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception during getTopTenHighlyReputedUsers(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);


            // 6.20 Task 20: Find Vulnerability Detection Publications
            addDivider(bufferedWriter);
            addInputTitle("Task 20: Vulnerability Detection Publications", bufferedWriter);
            try {
                QueryResult.TaskSpecificPublication[] pubs = ModelHubPlatform.getVulnerabilityDetectionPublications();
                if (pubs != null && pubs.length > 0) {
                    for (QueryResult.TaskSpecificPublication p : pubs) {
                        printLine(p.toString(), bufferedWriter);
                    }
                    printLine(pubs.length + " vulnerability detection publications found.", bufferedWriter);
                } else {
                    printLine("No vulnerability detection publications found.", bufferedWriter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                printLine("Exception during getVulnerabilityDetectionPublications(): " + e.toString(), bufferedWriter);
            }
            addDivider(bufferedWriter);



            // ===== END OF OUTPUT FILE =====
            printLine("========== OUTPUT FINISHED ==========", bufferedWriter);

            // CLOSE OUTPUT FILE
            if (bufferedWriter != null) bufferedWriter.close();



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedWriter != null) bufferedWriter.close();
            } catch (IOException ignored) {}

            // WAIT BEFORE SHUTDOWN
            try {
                System.out.println("DB is running. Press Enter to Quit...");
                System.in.read(); //waits an input
                // Keeps the server running until the user presses Enter.
                // While the server is running, you can connect to the H2 database on localhost
                // and execute SQL commands interactively for testing.
            } catch (IOException ignored) {}

            disconnect();

        }
    }
}
