package ceng.ceng351.ModelHubPlatform;

public class run {

    private int PIN;
    private int ModelID;
    private String version_no;
    private int DatasetID;
    private String run_type;

    public run(int PIN, int ModelID, String version_no, int DatasetID, String run_type) {
        this.PIN = PIN;
        this.ModelID = ModelID;
        this.version_no = version_no;
        this.DatasetID = DatasetID;
        this.run_type = run_type;
    }

    public int getPIN() {
        return PIN;
    }

    public void setPIN(int PIN) {
        this.PIN = PIN;
    }

    public int getModelID() {
        return ModelID;
    }

    public void setModelID(int ModelID) {
        this.ModelID = ModelID;
    }

    public String getVersion_no() {
        return version_no;
    }

    public void setVersion_no(String version_no) {
        this.version_no = version_no;
    }

    public int getDatasetID() {
        return DatasetID;
    }

    public void setDatasetID(int DatasetID) {
        this.DatasetID = DatasetID;
    }

    public String getRun_type() {
        return run_type;
    }

    public void setRun_type(String run_type) {
        this.run_type = run_type;
    }

    @Override
    public String toString() {
        return "run{" +
                "PIN=" + PIN +
                ", ModelID=" + ModelID +
                ", version_no='" + version_no + '\'' +
                ", DatasetID=" + DatasetID +
                ", run_type='" + run_type + '\'' +
                '}';
    }
}