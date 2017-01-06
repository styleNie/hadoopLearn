package bigdata.apriori;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by Administrator on 2016/12/26.
 */
public  class AprioriNode {

    public List<String> list = new ArrayList<String>();

    public AprioriNode(String str) {
        super();
        this.list = Arrays.asList(str.split(","));
        Collections.sort(this.list);
    }

    public AprioriNode() {
        super();
    }

    public boolean add(String s) {
        if (this.list.contains(s) == false) {
            this.list.add(s);
            Collections.sort(this.list);
            return true;
        } else {
            return false;
        }
    }

    public void remove(int i) {
        this.list.remove(i);
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

    public AprioriNode copy() {
        AprioriNode newobj = new AprioriNode();
        for (String term : this.list) {
            newobj.add(term);
        }
        return newobj;
    }

    public int size() {
        return this.list.size();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AprioriNode) {
            List<String> terms = ((AprioriNode) obj).getList();
            if (((AprioriNode) obj).size() == this.size()) {
                for (int i = 0, len = this.size(); i < len; i++) {
                    if (this.list.get(i).equals(terms.get(i)) == false) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (int i = 0, len = this.list.size(); i < len; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(this.list.get(i));
        }
        return sb.toString();
    }

    public String getFront(){
        return this.list.get(0);
    }

    public boolean isContained(AprioriNode obj){
        List<String> list=obj.getList();
        for(String b: list){
            if(this.list.contains(b)==false){
                return false;
            }
        }
        return true;
    }

}