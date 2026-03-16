package ceng.ceng351.ModelHubPlatform;

public class ModelVersion {

    private int ModelID;
    private String version_no;
    private String version_date;

    public ModelVersion(int ModelID, String version_no, String version_date) {
        this.ModelID = ModelID;
        this.version_no = version_no;
        this.version_date = version_date;
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

    public String getVersion_date() {
        return version_date;
    }

    public void setVersion_date(String version_date) {
        this.version_date = version_date;
    }

    @Override
    public String toString() {
        return "ModelVersion{" +
                "ModelID=" + ModelID +
                ", version_no='" + version_no + '\'' +
                ", version_date='" + version_date + '\'' +
                '}';
    }
}