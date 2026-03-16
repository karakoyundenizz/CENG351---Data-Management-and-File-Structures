package ceng.ceng351.ModelHubPlatform;

public class include {

    private int PubID;
    private int ResultID;
    private String placement_type;
    private String placement_section;

    public include(int PubID, int ResultID, String placement_type, String placement_section) {
        this.PubID = PubID;
        this.ResultID = ResultID;
        this.placement_type = placement_type;
        this.placement_section = placement_section;
    }

    public int getPubID() {
        return PubID;
    }

    public void setPubID(int PubID) {
        this.PubID = PubID;
    }

    public int getResultID() {
        return ResultID;
    }

    public void setResultID(int ResultID) {
        this.ResultID = ResultID;
    }

    public String getPlacement_type() {
        return placement_type;
    }

    public void setPlacement_type(String placement_type) {
        this.placement_type = placement_type;
    }

    public String getPlacement_section() {
        return placement_section;
    }

    public void setPlacement_section(String placement_section) {
        this.placement_section = placement_section;
    }

    @Override
    public String toString() {
        return "include{" +
                "PubID=" + PubID +
                ", ResultID=" + ResultID +
                ", placement_type='" + placement_type + '\'' +
                ", placement_section='" + placement_section + '\'' +
                '}';
    }
}
