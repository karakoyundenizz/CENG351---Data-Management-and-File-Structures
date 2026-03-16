package ceng.ceng351.ModelHubPlatform;

public class Publication {

    private int PubID;
    private String title;
    private String venue;

    public Publication(int PubID, String title, String venue) {
        this.PubID = PubID;
        this.title = title;
        this.venue = venue;
    }

    public int getPubID() {
        return PubID;
    }

    public void setPubID(int PubID) {
        this.PubID = PubID;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getVenue() {
        return venue;
    }

    public void setVenue(String venue) {
        this.venue = venue;
    }


    @Override
    public String toString() {
        return "Publication{" +
                "PubID=" + PubID +
                ", title='" + title + '\'' +
                ", venue='" + venue + '\'' +
                '}';
    }
}