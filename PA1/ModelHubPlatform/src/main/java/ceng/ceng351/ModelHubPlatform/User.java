package ceng.ceng351.ModelHubPlatform;

public class User {

    private int PIN;
    private String user_name;
    private int reputation_score;

    public User(int PIN, String user_name, int reputation_score) {
        this.PIN = PIN;
        this.user_name = user_name;
        this.reputation_score = reputation_score;
    }

    public int getPIN() {
        return PIN;
    }

    public void setPIN(int PIN) {
        this.PIN = PIN;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String p_name) {
        this.user_name = user_name;
    }

    public int getReputation_score() {
        return reputation_score;
    }

    public void setReputation_score(int age) {
        this.reputation_score = reputation_score;
    }

    @Override
    public String toString() {
        return "User{" +
                "PIN=" + PIN +
                ", user_name='" + user_name + '\'' +
                ", reputation_score=" + reputation_score +
                '}';
    }
}
