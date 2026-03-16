package ceng.ceng351.ModelHubPlatform;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/*
    FileOperations class for reading data files and writing output files.
 */
public class FileOperations {

    public static FileWriter createFileWriter(String path) throws IOException {
        File f = new File(path);

        FileWriter fileWriter = null;

        if (path.endsWith("/") || path.endsWith("\\")) {
            f.mkdirs();
        } else if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }

        if (!f.isDirectory() && f.exists()) {
            f.delete();
        }
        fileWriter = new FileWriter(f, false);

        return fileWriter;
    }

    // Helper method to get BufferedReader from resource
    private static BufferedReader getBufferedReader(String resourcePath) {
        InputStream inputStream = FileOperations.class.getClassLoader().getResourceAsStream(resourcePath);

        if (inputStream == null) {
            return null;
        }

        return new BufferedReader(new InputStreamReader(inputStream));
    }

    public static User[] readUsersFile() {
        List<User> usersList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/Users.txt")) {

            if (bufferedReader == null) {
                System.err.println("Users data file not found.");
                return new User[0];
            }

            String strLine;

            // Example strLine: PIN    user_name    reputation_score

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 3) {
                    System.out.println("There is a problem in Users File Reading phase");
                } else {
                    try {
                        int PIN = Integer.parseInt(words[0]);
                        String user_name = words[1];
                        int reputation_score = Integer.parseInt(words[2]);

                        User users = new User(PIN, user_name, reputation_score);
                        usersList.add(users);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in Users file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        return usersList.toArray(new User[0]);
    }

    public static Organization[] readOrganizationsFile() {
        List<Organization> organizationList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/Organizations.txt")) {

            if (bufferedReader == null) {
                System.err.println("Organizations data file not found.");
                return new Organization[0];
            }

            String strLine;

            // Example strLine: OrgID    org_name    rating

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 3) {
                    System.out.println("There is a problem in Organizations File Reading phase");
                } else {
                    try {
                        int OrgID = Integer.parseInt(words[0]);
                        String org_name = words[1];
                        double rating = Double.parseDouble(words[2]);

                        Organization organization = new Organization(OrgID, org_name, rating);
                        organizationList.add(organization);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in Organizations file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return organizationList.toArray(new Organization[0]);
    }

    public static Profile[] readProfilesFile() {
        List<Profile> profileList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/Profiles.txt")) {

            if (bufferedReader == null) {
                System.err.println("Profiles data file not found.");
                return new Profile[0];
            }

            String strLine;

            // Example strLine: ProfileID    bio    avatar_url    PIN

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 4) {
                    System.out.println("There is a problem in Profiles File Reading phase");
                } else {
                    try {
                        int ProfileID = Integer.parseInt(words[0]);
                        String bio = words[1];
                        String avatar_url = words[2];
                        int PIN = Integer.parseInt(words[3]);

                        Profile profile = new Profile(ProfileID, bio, avatar_url, PIN);
                        profileList.add(profile);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in Profiles file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return profileList.toArray(new Profile[0]);
    }

    public static Model[] readModelsFile() {
        List<Model> modelList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/Models.txt")) {

            if (bufferedReader == null) {
                System.err.println("Models data file not found.");
                return new Model[0];
            }

            String strLine;

            // Example strLine: ModelID    model_name    license    size    OrgID

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 5) {
                    System.out.println("There is a problem in Models File Reading phase");
                } else {
                    try {
                        int ModelID = Integer.parseInt(words[0]);
                        String model_name = words[1];
                        String license = words[2];
                        String size = words[3];
                        int OrgID = Integer.parseInt(words[4]);

                        Model model = new Model(ModelID, model_name, license, size, OrgID);
                        modelList.add(model);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in Models file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return modelList.toArray(new Model[0]);
    }

    public static ModelVersion[] readModelVersionsFile() {
        List<ModelVersion> modelVersionList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/ModelVersions.txt")) {

            if (bufferedReader == null) {
                System.err.println("ModelVersions data file not found.");
                return new ModelVersion[0];
            }

            String strLine;

            // Example strLine: ModelID    version_no    version_date

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 3) {
                    System.out.println("There is a problem in ModelVersions File Reading phase");
                } else {
                    try {
                        int ModelID = Integer.parseInt(words[0]);
                        String version_no = words[1];
                        String version_date = words[2];

                        ModelVersion modelVersion = new ModelVersion(ModelID, version_no, version_date);
                        modelVersionList.add(modelVersion);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in ModelVersions file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return modelVersionList.toArray(new ModelVersion[0]);
    }

    public static Task[] readTasksFile() {
        List<Task> taskList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/Tasks.txt")) {

            if (bufferedReader == null) {
                System.err.println("Tasks data file not found.");
                return new Task[0];
            }

            String strLine;

            // Example strLine: TaskID    task_name

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 2) {
                    System.out.println("There is a problem in Tasks File Reading phase");
                } else {
                    try {
                        int TaskID = Integer.parseInt(words[0]);
                        String task_name = words[1];

                        Task task = new Task(TaskID, task_name);
                        taskList.add(task);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in Tasks file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return taskList.toArray(new Task[0]);
    }

    public static Dataset[] readDatasetsFile() {
        List<Dataset> datasetList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/Datasets.txt")) {

            if (bufferedReader == null) {
                System.err.println("Datasets data file not found.");
                return new Dataset[0];
            }

            String strLine;

            // Example strLine: DatasetID    dataset_name    modality    number_of_rows

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 4) {
                    System.out.println("There is a problem in Datasets File Reading phase");
                } else {
                    try {
                        int DatasetID = Integer.parseInt(words[0]);
                        String dataset_name = words[1];
                        String modality = words[2];
                        int number_of_rows = Integer.parseInt(words[3]);

                        Dataset dataset = new Dataset(DatasetID, dataset_name, modality, number_of_rows);
                        datasetList.add(dataset);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in Datasets file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datasetList.toArray(new Dataset[0]);
    }

    public static Result[] readResultsFile() {
        List<Result> resultList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/Results.txt")) {

            if (bufferedReader == null) {
                System.err.println("Results data file not found.");
                return new Result[0];
            }

            String strLine;

            // Example strLine: ResultID    accuracy    f1_score    hyperparameter_config    PIN    ModelID    version_no    DatasetID

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 8) {
                    System.out.println("There is a problem in Results File Reading phase");
                } else {
                    try {
                        int ResultID = Integer.parseInt(words[0]);
                        double accuracy = Double.parseDouble(words[1]);
                        double f1_score = Double.parseDouble(words[2]);
                        String hyperparameter_config = words[3];
                        int PIN = Integer.parseInt(words[4]);
                        int ModelID = Integer.parseInt(words[5]);
                        String version_no = words[6];
                        int DatasetID = Integer.parseInt(words[7]);

                        Result result = new Result(ResultID, accuracy, f1_score, hyperparameter_config, PIN, ModelID, version_no, DatasetID);
                        resultList.add(result);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in Results file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList.toArray(new Result[0]);
    }

    public static Publication[] readPublicationsFile() {
        List<Publication> publicationList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/Publications.txt")) {

            if (bufferedReader == null) {
                System.err.println("Publications data file not found.");
                return new Publication[0];
            }

            String strLine;

            // Example strLine: PubID    title    venue

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 3) {
                    System.out.println("There is a problem in Publications File Reading phase");
                } else {
                    try {
                        int PubID = Integer.parseInt(words[0]);
                        String title = words[1];
                        String venue = words[2];

                        Publication publication = new Publication(PubID, title, venue);
                        publicationList.add(publication);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in Publications file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return publicationList.toArray(new Publication[0]);
    }

    public static follow[] readfollowsFile() {
        List<follow> followList = new ArrayList<>();

        // Assuming the file path is data/follows.txt based on the table name
        try (BufferedReader bufferedReader = getBufferedReader("data/follows.txt")) {

            if (bufferedReader == null) {
                System.err.println("follows data file not found.");
                return new follow[0];
            }

            String strLine;

            // Example strLine: followerPIN    followeePIN    following_date

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                // We expect 3 fields based on: follows (follower PIN, followee PIN, following date)
                if (words.length < 3) {
                    System.out.println("There is a problem in follows File Reading phase");
                } else {
                    try {
                        int followerPIN = Integer.parseInt(words[0]);
                        int followeePIN = Integer.parseInt(words[1]);
                        String following_date = words[2];

                        follow newFollow = new follow(followerPIN, followeePIN, following_date);
                        followList.add(newFollow);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in follows file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return followList.toArray(new follow[0]);
    }

    public static designed_for[] readdesigned_forsFile() {
        List<designed_for> designed_forList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/designed_for.txt")) {

            if (bufferedReader == null) {
                System.err.println("designed_for data file not found.");
                return new designed_for[0];
            }

            String strLine;

            // Example strLine: ModelID    TaskID    is_primary

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 3) {
                    System.out.println("There is a problem in designed_for File Reading phase");
                } else {
                    try {
                        int ModelID = Integer.parseInt(words[0]);
                        int TaskID = Integer.parseInt(words[1]);

                        // Boolean.parseBoolean returns true if the string is "true" (case-insensitive), else false.
                        boolean is_primary = Boolean.parseBoolean(words[2]);

                        designed_for newDesignedFor = new designed_for(ModelID, TaskID, is_primary);
                        designed_forList.add(newDesignedFor);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in designed_for file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return designed_forList.toArray(new designed_for[0]);
    }

    public static upload[] readuploadsFile() {
        List<upload> uploadList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/uploads.txt")) {

            if (bufferedReader == null) {
                System.err.println("uploads data file not found.");
                return new upload[0];
            }

            String strLine;

            // Example strLine: PIN    DatasetID    role

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 3) {
                    System.out.println("There is a problem in uploads File Reading phase");
                } else {
                    try {
                        int PIN = Integer.parseInt(words[0]);
                        int DatasetID = Integer.parseInt(words[1]);
                        String role = words[2];

                        upload newUpload = new upload(PIN, DatasetID, role);
                        uploadList.add(newUpload);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in uploads file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return uploadList.toArray(new upload[0]);
    }

    public static run[] readrunsFile() {
        List<run> runList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/runs.txt")) {

            if (bufferedReader == null) {
                System.err.println("runs data file not found.");
                return new run[0];
            }

            String strLine;

            // Example strLine: PIN    ModelID    version_no    DatasetID    run_type

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 5) {
                    System.out.println("There is a problem in runs File Reading phase");
                } else {
                    try {
                        int PIN = Integer.parseInt(words[0]);
                        int ModelID = Integer.parseInt(words[1]);
                        String version_no = words[2];
                        int DatasetID = Integer.parseInt(words[3]);
                        String run_type = words[4];

                        run newRun = new run(PIN, ModelID, version_no, DatasetID, run_type);
                        runList.add(newRun);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in runs file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return runList.toArray(new run[0]);
    }

    public static include[] readincludesFile() {
        List<include> includeList = new ArrayList<>();

        try (BufferedReader bufferedReader = getBufferedReader("data/includes.txt")) {

            if (bufferedReader == null) {
                System.err.println("includes data file not found.");
                return new include[0];
            }

            String strLine;

            // Example strLine: PubID    ResultID    placement_type    placement_section

            // Skip the header line
            bufferedReader.readLine();

            while ((strLine = bufferedReader.readLine()) != null) {
                // Parse strLine
                String[] words = strLine.split("\t");

                if (words.length < 4) {
                    System.out.println("There is a problem in includes File Reading phase");
                } else {
                    try {
                        int PubID = Integer.parseInt(words[0]);
                        int ResultID = Integer.parseInt(words[1]);
                        String placement_type = words[2];
                        String placement_section = words[3];

                        include newInclude = new include(PubID, ResultID, placement_type, placement_section);
                        includeList.add(newInclude);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format in includes file: " + strLine);
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return includeList.toArray(new include[0]);
    }

}
