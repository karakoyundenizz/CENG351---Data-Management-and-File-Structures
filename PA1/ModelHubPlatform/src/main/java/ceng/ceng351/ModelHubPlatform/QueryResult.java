package ceng.ceng351.ModelHubPlatform;


public class QueryResult {

    //6.5 Task 5: Find Users With Specific Bio Keywords
    public static class UserPINNameReputationBio {

        private int PIN;
        private String user_name;
        private int reputation_score;
        private String bio;

        public UserPINNameReputationBio(int PIN, String user_name, int reputation_score, String bio) {
            this.PIN = PIN;
            this.user_name = user_name;
            this.reputation_score = reputation_score;
            this.bio = bio;
        }

        @Override
        public String toString() {
            return "UserPINNameReputationBio{" +
                    "PIN=" + PIN +
                    ", user_name='" + user_name + '\'' +
                    ", reputation_score=" + reputation_score +
                    ", bio='" + bio + '\'' +
                    '}';
        }
    }


    // 6.8 Task 8: Retrieve Models and Their Primary Task Information
    public static class ModelPrimaryTaskInfo {

        private int ModelID;
        private String model_name;
        private String primary_task_name;
        private int primary_task_count;

        public ModelPrimaryTaskInfo(int ModelID, String model_name, String primary_task_name, int primary_task_count) {
            this.ModelID = ModelID;
            this.model_name = model_name;
            this.primary_task_name = primary_task_name;
            this.primary_task_count = primary_task_count;
        }

        @Override
        public String toString() {
            return "ModelPrimaryTaskInfo{" +
                    "ModelID=" + ModelID +
                    ", model_name='" + model_name + '\'' +
                    ", primary_task_name='" + primary_task_name + '\'' +
                    ", primary_task_count=" + primary_task_count +
                    '}';
        }
    }


    // 6.9 Task 9: Compute User Popularity Score
    public static class UserPopularityInfo {

        private int PIN;
        private String user_name;
        private int popularity_score;

        public UserPopularityInfo(int PIN, String user_name, int popularity_score) {
            this.PIN = PIN;
            this.user_name = user_name;
            this.popularity_score = popularity_score;
        }

        @Override
        public String toString() {
            return "UserPopularityInfo{" +
                    "PIN=" + PIN +
                    ", user_name='" + user_name + '\'' +
                    ", popularity_score=" + popularity_score +
                    '}';
        }
    }


    // 6.10 Task 10: Comprehensive Model Information
    public static class ComprehensiveModelInfo {

        private int ModelID;
        private String model_name;
        private String org_name;
        private String license;
        private String size;
        private String primary_task_name;
        private int total_number_of_versions;
        private String latest_version_no;
        private String latest_version_date;

        public ComprehensiveModelInfo(int ModelID, String model_name, String org_name,
                                      String license, String size, String primary_task_name,
                                      int total_number_of_versions, String latest_version_no,
                                      String latest_version_date) {

            this.ModelID = ModelID;
            this.model_name = model_name;
            this.org_name = org_name;
            this.license = license;
            this.size = size;
            this.primary_task_name = primary_task_name;
            this.total_number_of_versions = total_number_of_versions;
            this.latest_version_no = latest_version_no;
            this.latest_version_date = latest_version_date;
        }

        @Override
        public String toString() {
            return "ComprehensiveModelInfo{" +
                    "ModelID=" + ModelID +
                    ", model_name='" + model_name + '\'' +
                    ", org_name='" + org_name + '\'' +
                    ", license='" + license + '\'' +
                    ", size='" + size + '\'' +
                    ", primary_task_name='" + primary_task_name + '\'' +
                    ", total_number_of_versions=" + total_number_of_versions +
                    ", latest_version_no='" + latest_version_no + '\'' +
                    ", latest_version_date='" + latest_version_date + '\'' +
                    '}';
        }
    }


    // 6.11 Task 11: Dataset Statistics by Modality
    public static class DatasetStatisticsByModality {

        private String modality;
        private int dataset_count;
        private double average_rows;

        public DatasetStatisticsByModality(String modality, int dataset_count, double average_rows) {
            this.modality = modality;
            this.dataset_count = dataset_count;
            this.average_rows = Math.round(average_rows * 100.0) / 100.0;  // <-- 2 decimal points
        }

        @Override
        public String toString() {
            return "DatasetStatisticsByModality{" +
                    "modality='" + modality + '\'' +
                    ", dataset_count=" + dataset_count +
                    ", average_rows=" + String.format("%.2f", average_rows) +
                    '}';
        }
    }


    // 6.12 Task 12: Retrieve Large-Parameter Model Versions Within a Date Range
    public static class LargeModelVersionInfo {

        private int ModelID;
        private String model_name;
        private String size;
        private String version_no;
        private String version_date;

        public LargeModelVersionInfo(int ModelID, String model_name, String size,
                                     String version_no, String version_date) {
            this.ModelID = ModelID;
            this.model_name = model_name;
            this.size = size;
            this.version_no = version_no;
            this.version_date = version_date;
        }

        @Override
        public String toString() {
            return "LargeModelVersionInfo{" +
                    "ModelID=" + ModelID +
                    ", model_name='" + model_name + '\'' +
                    ", size='" + size + '\'' +
                    ", version_no='" + version_no + '\'' +
                    ", version_date='" + version_date + '\'' +
                    '}';
        }
    }


    // 6.13 Task 13: Find Dataset(s) With Maximum Upload Count
    public static class DatasetMaxUploadInfo {

        private int DatasetID;
        private String dataset_name;
        private int upload_count;

        public DatasetMaxUploadInfo(int DatasetID, String dataset_name, int upload_count) {
            this.DatasetID = DatasetID;
            this.dataset_name = dataset_name;
            this.upload_count = upload_count;
        }

        @Override
        public String toString() {
            return "DatasetMaxUploadInfo{" +
                    "DatasetID=" + DatasetID +
                    ", dataset_name='" + dataset_name + '\'' +
                    ", upload_count=" + upload_count +
                    '}';
        }
    }


    // 6.16 Task 16: Find Users Who Ran All Versions of at Least One Model
    public static class UserModelVersionInfo {

        private int PIN;
        private String user_name;
        private int ModelID;
        private String model_name;
        private String version_no;
        private String license;

        public UserModelVersionInfo(int PIN, String user_name,
                                    int ModelID, String model_name,
                                    String version_no, String license) {
            this.PIN = PIN;
            this.user_name = user_name;
            this.ModelID = ModelID;
            this.model_name = model_name;
            this.version_no = version_no;
            this.license = license;
        }

        @Override
        public String toString() {
            return "UserModelVersionInfo{" +
                    "PIN=" + PIN +
                    ", user_name='" + user_name + '\'' +
                    ", ModelID=" + ModelID +
                    ", model_name='" + model_name + '\'' +
                    ", version_no='" + version_no + '\'' +
                    ", license='" + license + '\'' +
                    '}';
        }
    }


    // 6.17 Task 17: Run-Type Statistics
    public static class RunTypeStats {

        private String run_type;
        private int total_number_of_results;
        private double average_f1_score;

        public RunTypeStats(String run_type, int total_number_of_results, double average_f1_score) {
            this.run_type = run_type;
            this.total_number_of_results = total_number_of_results;
            this.average_f1_score = Math.round(average_f1_score * 100.0) / 100.0;  // <-- 2 decimal points
        }

        @Override
        public String toString() {
            return "RunTypeStats{" +
                    "run_type='" + run_type + '\'' +
                    ", total_number_of_results=" + total_number_of_results +
                    ", average_f1_score=" + String.format("%.2f", average_f1_score) +
                    '}';
        }
    }


    // 6.19 Task 19: Find Top 10 Highly-Reputed Users
    public static class HighlyReputedUser {

        private int PIN;
        private String user_name;
        private int user_score;

        public HighlyReputedUser(int PIN, String user_name, int user_score) {
            this.PIN = PIN;
            this.user_name = user_name;
            this.user_score = user_score;
        }

        @Override
        public String toString() {
            return "HighlyReputedUser{" +
                    "PIN=" + PIN +
                    ", user_name='" + user_name + '\'' +
                    ", user_score=" + user_score +
                    '}';
        }
    }


    // 6.20 Task 20: Find Vulnerability Detection Publications
    public static class TaskSpecificPublication {
        private int pubID;
        private int resultID;
        private String title;
        private String venue;
        private String run_type;
        private double f1_score;
        private double accuracy;
        private String hyperparameter_config;
        private String placement_type;
        private String placement_section;
        private String user_name;
        private String model_name;
        private String size;
        private String version_no;
        private String dataset_name;

        public TaskSpecificPublication(int pubID, int resultID, String title, String venue,
                                       String run_type,
                                       double f1_score, double accuracy,
                                       String hyperparameter_config,
                                       String placement_type, String placement_section,
                                       String user_name, String model_name, String size,
                                       String version_no, String dataset_name) {

            this.pubID = pubID;
            this.resultID = resultID;
            this.title = title;
            this.venue = venue;
            this.run_type = run_type;
            this.f1_score = Math.round(f1_score * 100.0) / 100.0;  // <-- 2 decimal points
            this.accuracy = Math.round(accuracy * 100.0) / 100.0;  // <-- 2 decimal points
            this.hyperparameter_config = hyperparameter_config;
            this.placement_type = placement_type;
            this.placement_section = placement_section;
            this.user_name = user_name;
            this.model_name = model_name;
            this.size = size;
            this.version_no = version_no;
            this.dataset_name = dataset_name;
        }

        @Override
        public String toString() {
            return "TaskSpecificPublication{" +
                    "PubID=" + pubID +
                    ", ResultID=" + resultID +
                    ", title='" + title + '\'' +
                    ", venue='" + venue + '\'' +
                    ", run_type='" + run_type + '\'' +
                    ", f1_score=" + String.format("%.2f", f1_score) +
                    ", accuracy=" + String.format("%.2f", accuracy) +
                    ", hyperparameter_config='" + hyperparameter_config + '\'' +
                    ", placement_type='" + placement_type + '\'' +
                    ", placement_section='" + placement_section + '\'' +
                    ", user_name='" + user_name + '\'' +
                    ", model_name='" + model_name + '\'' +
                    ", size='" + size + '\'' +
                    ", version_no='" + version_no + '\'' +
                    ", dataset_name='" + dataset_name + '\'' +
                    '}';
        }
    }


}
