package ceng.ceng351.ModelHubPlatform;

public class designed_for {

    private int ModelID;
    private int TaskID;
    private boolean is_primary;

    public designed_for(int ModelID, int TaskID, boolean is_primary) {
        this.ModelID = ModelID;
        this.TaskID = TaskID;
        this.is_primary = is_primary;
    }

    public int getModelID() {
        return ModelID;
    }

    public void setModelID(int ModelID) {
        this.ModelID = ModelID;
    }

    public int getTaskID() {
        return TaskID;
    }

    public void setTaskID(int TaskID) {
        this.TaskID = TaskID;
    }

    public boolean isIs_primary() {
        return is_primary;
    }

    public void setIs_primary(boolean is_primary) {
        this.is_primary = is_primary;
    }

    @Override
    public String toString() {
        return "designed_for{" +
                "ModelID=" + ModelID +
                ", TaskID=" + TaskID +
                ", is_primary=" + is_primary +
                '}';
    }
}