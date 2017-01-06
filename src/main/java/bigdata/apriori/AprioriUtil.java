package bigdata.apriori;

/**
 * Created by Administrator on 2016/12/25.
 */

import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;

public class AprioriUtil {

    public static String List2String(List<String> list) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0, len = list.size(); i < len; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(list.get(i));
        }
        return sb.toString();
    }

    public static List<AprioriNode> genSubNodes(AprioriNode node) {
        List<AprioriNode> res = new ArrayList<AprioriNode>();
        for (int i = 0, len = node.size(); i < len; i++) {
            AprioriNode temp = node.copy();
            temp.remove(i);
            res.add(temp);
        }
        return res;
    }

    public static List<AprioriNode> genSubNodes(AprioriNode node, int k) {
        List<AprioriNode> res = bfs(node, k);
        return res;
    }

    private static class Node {
        private String value = "";
        private int k = 0;
        private int curpos = 0;

        public Node(String value, int k, int curpos) {
            super();
            this.value = value;
            this.k = k;
            this.curpos = curpos;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }

        public int getCurpos() {
            return curpos;
        }

        public void setCurpos(int curpos) {
            this.curpos = curpos;
        }

        @Override
        public String toString() {
            return "Node [value=" + value + ", k=" + k + ", curpos=" + curpos + "]";
        }
    }

    public static List<AprioriNode> bfs(AprioriNode node, int k) {
//        void push(E e):将给定元素”压入”栈中。存入的元素会在栈首。即:栈的第一个元素
//        E pop():将栈首元素删除并返回。
        List<AprioriNode> res = new ArrayList<AprioriNode>();
        List<String> list = node.getList();
        //Queue<Node> queue = new Queue<Node>();
        LinkedList<Node> queue = new LinkedList<Node>();
        //queue.push(new Node("", 0, 0));
        queue.addFirst(new Node("",0,0));
        while (queue.isEmpty() == false) {
            //Node head = queue.pop();
            Node head = queue.removeFirst();
            if (head.getK() >= k) {
                res.add(new AprioriNode(head.getValue()));
            } else if (head.getCurpos() >= list.size()) {
                continue;
            } else {
                int head_k = head.getK();
                String head_value = head.getValue();
                int i = head.getCurpos();
                if (head_k < k) {
                    if ("".equals(head_value)) {
                        //queue.push(new Node(head_value + list.get(i) + ",", head_k + 1, i + 1));
                        queue.addFirst(new Node(head_value + list.get(i) + ",", head_k + 1, i + 1));
                    } else {
                        //queue.push(new Node(head_value + list.get(i), head_k + 1, i + 1));
                        queue.addFirst(new Node(head_value + list.get(i), head_k + 1, i + 1));
                    }
                }
                //queue.push(new Node(head_value, head_k, i + 1));
                queue.addFirst(new Node(head_value, head_k, i + 1));
            }
        }
        return res;
    }

    public static void main(String[] args) {
        List<AprioriNode> subNodes = genSubNodes(new AprioriNode("1,2,3,4"));
        System.out.println(subNodes);
    }
}
