package ceng.ceng351.ModelHubPlatform;

public class Dataset {

    private int DatasetID;
    private String dataset_name;
    private String modality;
    private int number_of_rows;

    public Dataset(int DatasetID, String dataset_name, String modality, int number_of_rows) {
        this.DatasetID = DatasetID;
        this.dataset_name = dataset_name;
        this.modality = modality;
        this.number_of_rows = number_of_rows;
    }

    public int getDatasetID() {
        return DatasetID;
    }

    public void setDatasetID(int DatasetID) {
        this.DatasetID = DatasetID;
    }

    public String getDataset_name() {
        return dataset_name;
    }

    public void setDataset_name(String dataset_name) {
        this.dataset_name = dataset_name;
    }

    public String getModality() {
        return modality;
    }

    public void setModality(String modality) {
        this.modality = modality;
    }

    public int getNumber_of_rows() {
        return number_of_rows;
    }

    public void setNumber_of_rows(int number_of_rows) {
        this.number_of_rows = number_of_rows;
    }

    @Override
    public String toString() {
        return "Dataset{" +
                "DatasetID=" + DatasetID +
                ", dataset_name='" + dataset_name + '\'' +
                ", modality='" + modality + '\'' +
                ", number_of_rows=" + number_of_rows +
                '}';
    }
}
