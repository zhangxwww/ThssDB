package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.util.HashMap;

enum CompareType {
    LT, LE, EQ, GT, GE, NE;
    static HashMap<String,CompareType> map= new HashMap<String,CompareType>(){
        {
            put("<",LT);
            put("<=",LE);
            put(">",GT);
            put(">=",GE);
            put("=",EQ);
            put("<>",NE);
        }
    };
    public static CompareType parseOp(String s){
        return  map.get(s);
    }
}

public class AttrCompare {
    CompareType type;
    public AttrCompare(String type) {
        this.type = CompareType.parseOp(type);
    }

    public boolean eval(Entry x, Entry y){
        boolean result;
        switch(type){
            case EQ:
                result= x.equals(y);
                break;
            case LE:
                result= x.compareTo(y) <= 0;
                break;
            case LT:
                result= x.compareTo(y) < 0;
                break;
            case GT:
                result= x.compareTo(y) > 0;
                break;
            case GE:
                result= x.compareTo(y) >= 0;
                break;
            case NE:
                result= !x.equals(y);
                break;
            default:
                result = false;
                break;
        }
        return result;
    }
}