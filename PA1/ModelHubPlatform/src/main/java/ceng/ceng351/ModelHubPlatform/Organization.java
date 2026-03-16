package ceng.ceng351.ModelHubPlatform;

public class Organization {

    private int OrgID;
    private String org_name;
    private double rating;

    public Organization(int OrgID, String org_name, double rating) {
        this.OrgID = OrgID;
        this.org_name = org_name;
        this.rating = Math.round(rating * 100.0) / 100.0;
    }

    public int getOrgID() {
        return OrgID;
    }

    public void setOrgID(int OrgID) {
        this.OrgID = OrgID;
    }

    public String getOrg_name() {
        return org_name;
    }

    public void setOrg_name(String org_name) {
        this.org_name = org_name;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    @Override
    public String toString() {
        return "Organization{" +
                "OrgID=" + OrgID +
                ", org_name='" + org_name + '\'' +
                ", rating=" + String.format("%.2f", rating) +
                '}';
    }
}
