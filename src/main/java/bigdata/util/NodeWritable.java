package bigdata.util;

/**
 * Created by Administrator on 2016/12/26.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class NodeWritable implements Writable, WritableComparable<NodeWritable> {

    public static final int M = 1;   //表示第一个矩阵中的元素
    public static final int N = 2;   //表示第二个矩阵中的元素
    private Integer flag = null;
    private Integer i = null;
    private Integer j = null;
    private Long val = null;

    public static class Node implements Comparable<Node> {
        private Integer i = null;
        private Integer j = null;
        private Long val = null;

        public Node(Integer i, Integer j, Long val) {
            super();
            this.i = i;
            this.j = j;
            this.val = val;
        }

        public Integer getI() {
            return i;
        }

        public Integer getJ() {
            return j;
        }

        public Long getVal() {
            return val;
        }

        @Override
        public String toString() {
            return "Node [i=" + i + ", j=" + j + ", val=" + val + "]";
        }

        //@Override
        public int compareTo(Node o) {
            if (this.getI() == o.getI()) {
                return this.getJ() - o.getJ();
            } else {
                return this.getI() - o.getI();
            }
        }
    }

    //@Override
    public void readFields(DataInput in) throws IOException {
        flag = in.readInt();
        i = in.readInt();
        j = in.readInt();
        val = in.readLong();
    }

    //@Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(flag);
        out.writeInt(i);
        out.writeInt(j);
        out.writeLong(val);
    }

    //@Override
    public int compareTo(NodeWritable o) {
        if (this.getI() == o.getI()) {
            return this.getJ() - o.getJ();
        } else {
            return this.getI() - o.getI();
        }
    }

    public Integer getFlag() {
        return flag;
    }

    public Integer getI() {
        return i;
    }

    public Integer getJ() {
        return j;
    }

    public Long getVal() {
        return val;
    }

    public NodeWritable(Integer flag, Integer i, Integer j, Long val) {
        super();
        this.flag = flag;
        this.i = i;
        this.j = j;
        this.val = val;
    }

    public NodeWritable() {
        super();
        this.flag = 0;
        this.i = 0;
        this.j = 0;
        this.val = 0L;
    }

    @Override
    public String toString() {
        return "NodeWritable [flag=" + flag + ", i=" + i + ", j=" + j
                + ", val=" + val + "]";
    }

    public Node getNode() {
        return new Node(i, j, val);
    }

}
