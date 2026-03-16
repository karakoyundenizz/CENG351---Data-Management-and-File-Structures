package ceng.ceng351.ModelHubPlatform;

public class Profile {

    private int ProfileID;
    private String bio;
    private String avatar_url;
    private int PIN;

    public Profile(int ProfileID, String bio, String avatar_url, int PIN) {
        this.ProfileID = ProfileID;
        this.bio = bio;
        this.avatar_url = avatar_url;
        this.PIN = PIN;
    }

    public int getProfileID() {
        return ProfileID;
    }

    public void setProfileID(int ProfileID) {
        this.ProfileID = ProfileID;
    }

    public String getBio() {
        return bio;
    }

    public void setBio(String bio) {
        this.bio = bio;
    }

    public String getAvatar_url() {
        return avatar_url;
    }

    public void setAvatar_url(String avatar_url) {
        this.avatar_url = avatar_url;
    }

    public int getPIN() {
        return PIN;
    }

    public void setPIN(int PIN) {
        this.PIN = PIN;
    }

    @Override
    public String toString() {
        return "Profile{" +
                "ProfileID=" + ProfileID +
                ", bio='" + bio + '\'' +
                ", avatar_url='" + avatar_url + '\'' +
                ", PIN=" + PIN +
                '}';
    }
}