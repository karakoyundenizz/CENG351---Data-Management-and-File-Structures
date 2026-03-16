package ceng.ceng351.ModelHubPlatform;

import java.sql.Connection;

public interface IModelHubPlatform {


    /**
     * This function will be called before all other operations. If your implementation
     * requires initialization, it should be done inside this function. The Evaluation
     * class provides the database Connection object, and your implementation should
     * store and use this connection for all subsequent database operations, including
     * creating Statements or PreparedStatements.
     */
    public void initialize(Connection connection);



    /**
     * Should drop the database tables if they exist.
     *
     * @return The number of tables that are dropped successfully.
     */
    public int dropTables();
    
    /**
     * 6.1 Task 1: Create Database Tables
     * Should create the necessary tables when called.
     *
     * @return the number of tables that are created successfully.
     */
    public int createTables();

    /**
     * 6.2 Task 2: Insert Users
     * Should insert an array of Users into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertUsers(User[] users);

    /**
     * 6.2 Task 2: Insert Organizations
     * Should insert an array of Organizations into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertOrganizations(Organization[] organizations);

    /**
     * 6.2 Task 2: Insert Tasks
     * Should insert an array of Tasks into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertTasks(Task[] tasks);

    /**
     * 6.2 Task 2: Insert Datasets
     * Should insert an array of Datasets into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertDatasets(Dataset[] datasets);

    /**
     * 6.2 Task 2: Insert Publications
     * Should insert an array of Publications into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertPublications(Publication[] publications);

    /**
     * 6.2 Task 2: Insert Profiles
     * Should insert an array of Profiles into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertProfiles(Profile[] profiles);

    /**
     * 6.2 Task 2: Insert follows
     * Should insert an array of follows into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertfollows(follow[] follows);

    /**
     * 6.2 Task 2: Insert Models
     * Should insert an array of Models into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertModels(Model[] models);

    /**
     * 6.2 Task 2: Insert uploads
     * Should insert an array of uploads into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertuploads(upload[] uploads);

    /**
     * 6.2 Task 2: Insert ModelVersions
     * Should insert an array of ModelVersions into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertModelVersions(ModelVersion[] modelVersions);

    /**
     * 6.2 Task 2: Insert designed_for
     * Should insert an array of designed_fors into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertdesigned_fors(designed_for[] designed_fors);

    /**
     * 6.2 Task 2: Insert runs
     * Should insert an array of runs into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertruns(run[] runs);

    /**
     * 6.2 Task 2: Insert Results
     * Should insert an array of Results into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertResults(Result[] results);

    /**
     * 6.2 Task 2: Insert includes
     * Should insert an array of includes into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertincludes(include[] includes);

    /**
     * 6.3 Task 3: Find Users Without Profiles
     * Retrieves an array of User objects who do not have a corresponding profile.
     *
     * Results must be sorted by reputation_score in descending order.
     * If two users have the same reputation_score, they must be sorted by PIN in ascending order.
     *
     * @return An array of User objects.
     */
    public User[] getUsersWithoutProfiles();

    /**
     * 6.4 Task 4: Decrease Reputation for Users Without Profiles
     * Decreases the reputation score of users who do not have a profile.
     * Only users with reputation_score >= 10 and without profile are updated (decrease by 10).
     *
     * @return The number of users whose reputation score was decreased.
     */
    public int decreaseReputationForMissingProfiles();

    /**
     * 6.5 Task 5: Find Users With Specific Bio Keywords
     * Gets all users who have a profile and whose bio contains at least one of the
     * following case-insensitive keywords: "engineer", "scientist", or "student".
     *
     * Results must be sorted by PIN in ascending order.
     *
     * @return An array of UserPINNameReputationBio objects containing PIN, user_name, reputation_score, and bio.
     */
    public QueryResult.UserPINNameReputationBio[] getUsersByBioKeywords();

    /**
     * 6.6 Task 6: Find Organizations With No Released Models and Low Rating
     * Gets all organizations that have not released any models and whose rating is
     * strictly smaller than 2.5 (i.e., rating < 2.5).
     * Organizations with rating = 2.5 must NOT be included.
     *
     * Results must be sorted by OrgID in descending order.
     *
     * @return An array of Organization objects containing OrgID, org_name, and rating.
     */
    public Organization[] getOrganizationsWithNoReleasedModelsAndLowRating();

    /**
     * 6.7 Task 7: Delete Organizations With No Released Models and Low Rating
     * Deletes all organizations that have not released any models and whose rating is
     * strictly smaller than 2.5 (i.e., rating < 2.5).
     * Organizations with rating = 2.5 must NOT be deleted.
     *
     * @return The number of deleted rows.
     */
    public int deleteOrganizationsWithNoReleasedModelsAndLowRating();

    /**
     * 6.8 Task 8: Retrieve Models and Their Primary Task Information
     * Retrieves each model together with its primary task information.
     * For every model, returns ModelID, model_name, the model's primary_task_name,
     * and the number of primary tasks assigned to that model (primary_task_count).
     *
     * Results must be sorted by ModelID in ascending order.
     *
     * @return An array of ModelPrimaryTaskInfo objects.
     */
    public QueryResult.ModelPrimaryTaskInfo[] getModelPrimaryTaskInfo();

    /**
     * 6.9 Task 9: Compute User Popularity Score
     * Computes the popularity score for all users, defined as:
     * popularity_score = follower_count - followee_count.
     *
     * If a user has no followers, follower_count must be treated as 0.
     * If a user follows no one, followee_count must be treated as 0.
     *
     * Returns the top 20 users sorted by popularity score (descending),
     * and by PIN ascending when scores are equal.
     *
     * @return An array of UserPopularityInfo objects containing
     *         PIN, user_name, and popularity_score.
     */
    public QueryResult.UserPopularityInfo[] getUserPopularityScore();

    /**
     * 6.10 Task 10: Comprehensive Model Information
     * Retrieves comprehensive model information, including:
     * ModelID, model_name, org_name, license, size, primary_task_name,
     * total_number_of_versions, latest_version_no, and latest_version_date.
     *
     * Results must be sorted by ModelID in ascending order.
     *
     * @return An array of ComprehensiveModelInfo objects.
     */
    public QueryResult.ComprehensiveModelInfo[] getComprehensiveModelInfo();

    /**
     * 6.11 Task 11: Dataset Statistics by Modality
     * Computes, for each modality, the total number of datasets and the
     * average number of rows.
     *
     * Results are sorted by average_rows in descending order, and by modality ascending when equal.
     *
     * @return An array of DatasetStatisticsByModality objects.
     */
    public QueryResult.DatasetStatisticsByModality[] getDatasetStatisticsByModality();

    /**
     * 6.12 Task 12: Retrieve Large-Parameter Model Versions Within a Date Range
     * Retrieves model versions of large-parameter models (size ending with 'B')
     * whose version_date falls within the given date range (inclusive).
     *
     * Results must be sorted by ModelID in ascending order and version_date in ascending order.
     *
     * @param start_date The beginning of the date range (YYYY-MM-DD).
     * @param end_date   The end of the date range (YYYY-MM-DD).
     * @return An array of LargeModelVersionInfo objects.
     */
    public QueryResult.LargeModelVersionInfo[] getLargeModelVersionsByDateRange(String start_date, String end_date);

    /**
     * 6.13 Task 13: Find Dataset(s) With Maximum Upload Count
     * Finds the dataset(s) that have the maximum number of uploads.
     * Output includes DatasetID, dataset_name, and upload_count.
     *
     * Result must be sorted by DatasetID in ascending order.
     *
     * @return array of DatasetMaxUploadInfo objects
     */
    public QueryResult.DatasetMaxUploadInfo[] getDatasetsWithMaxUploadCount();

    /**
     * 6.14 Task 14: Find Complete Dataset(s) with All Roles
     * Finds all datasets that have been uploaded by users from every role in the system (i.e., complete datasets).
     *
     * Result must be sorted by DatasetID in ascending order.
     *
     * @return array of Dataset objects
     */
    public Dataset[] getCompleteDatasets();

    /**
     * 6.15 Task 15: Find Users Who Uploaded Datasets with Role 'creator' or 'contributor' but Never 'validator' and Have Reputation ≥ 60
     * Retrieves all users who uploaded at least one dataset with role 'creator'
     * or 'contributor', never uploaded any dataset with role 'validator',
     * and have a reputation score greater than or equal to 60.
     *
     * Result must be sorted by PIN in ascending order.
     *
     * @return array of User objects
     */
    public User[] getUsersCreatorOrContributorButNotValidator();

    /**
     * 6.16 Task 16: Find Users Who Ran All Versions of at Least One Model
     * Retrieves all users who have executed runs for ALL versions of
     * at least one model. A user is included if they ran every version
     * of any model.
     *
     * Output includes: PIN, user_name, ModelID, model_name,
     * version_no, and license for the model that satisfies the condition.
     *
     * Results must be sorted in ascending order by PIN, ModelID, and version_no.
     *
     * @return array of UserModelVersionInfo objects
     */
    public QueryResult.UserModelVersionInfo[] getUsersWhoRanAllVersionsOfModels();

    /**
     * 6.17 Task 17: Run-Type Statistics
     * Computes summary statistics for each distinct run_type, including:
     * - total_number_of_results: the number of results created by runs of that run_type
     * - average_f1_score: the average f1_score computed from results that were created by runs of that run_type
     *
     * Results must be returned sorted by run_type in DESCENDING order.
     *
     * @return QueryResult.RunTypeStats[]
     */
    public QueryResult.RunTypeStats[] getRunTypeStatistics();

    /**
     * 6.18 Task 18: Find Publications That Include Results From Runs of a Dataset
     * Returns all publications that include at least one result
     * created by a run that used the specified dataset_name.
     * A result must have accuracy value greater than or equal to 0.70.
     *
     * Result must be sorted by PubID in ascending order.
     *
     * @param dataset_name the dataset name to filter by
     * @return an array of Publication objects
     */
    public Publication[] getPublicationsUsingDataset(String dataset_name);

    /**
     * 6.19 Task 19: Find Top 10 Highly-Reputed Users
     * Computes user_score for all users as:
     *   number_of_owner_uploads + number_of_publications_including_user_results + reputation_score.
     * Output must contain the fields PIN, user_name, and user_score.
     *
     * Missing counts are treated as zero using CASE WHEN.
     *
     * Returns the top 10 users with the highest user_score, sorted by user_score
     * in DESCENDING order and by PIN in ASCENDING order for ties.
     *
     * @return QueryResult.HighlyReputedUser[]
     */
    public QueryResult.HighlyReputedUser[] getTopTenHighlyReputedUsers();

    /**
     * 6.20 Task 20: Find Vulnerability Detection Publications
     * Retrieves all publications that include results created by runs of model
     * versions whose associated task_name is "Vulnerability Detection", and whose
     * results have accuracy >= 0.70 and f1_score >= 0.70.
     *
     * Returned objects contain the following fields:
     * - PubID
     * - ResultID
     * - title
     * - venue
     * - run_type
     * - f1_score
     * - accuracy
     * - hyperparameter_config
     * - placement_type
     * - placement_section
     * - user_name
     * - model_name
     * - size
     * - version_no
     * - dataset_name
     *
     * Results must be sorted by PubID ASC, then ResultID ASC.
     *
     * @return QueryResult.TaskSpecificPublication[]
     */
    public QueryResult.TaskSpecificPublication[] getVulnerabilityDetectionPublications();


}
