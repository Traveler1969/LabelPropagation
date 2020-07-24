import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NovelViewBean implements WritableComparable<NovelViewBean> {
    private int tag = -1;
    private String novelName = "";
    private String peopleName = "";

    @Override
    public int compareTo(NovelViewBean o) {
        if(this.tag != o.tag) {
            return Integer.compare(this.tag, o.tag);
        } else if(!this.novelName.equals(o.novelName)) {
            return -this.novelName.compareTo(o.novelName);
        } else {
            return this.peopleName.compareTo(o.peopleName);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(tag);
        dataOutput.writeUTF(novelName);
        dataOutput.writeUTF(peopleName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag = dataInput.readInt();
        novelName = dataInput.readUTF();
        peopleName = dataInput.readUTF();
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(tag);
    }

    public int getTag() {
        return tag;
    }

    public void setTag(int tag) {
        this.tag = tag;
    }

    public String getNovelName() {
        return novelName;
    }

    public void setNovelName(String novelName) {
        this.novelName = novelName;
    }

    public String getPeopleName() {
        return peopleName;
    }

    public void setPeopleName(String peopleName) {
        this.peopleName = peopleName;
    }
}
