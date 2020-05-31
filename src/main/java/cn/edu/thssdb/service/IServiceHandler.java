package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import javax.xml.bind.annotation.XmlElementDecl;
import java.util.*;

public class IServiceHandler implements IService.Iface {

    private Set<Long> connected_sessionid = new HashSet<>();
    private HashMap<Long, StatementExecuter> executerList;
    private Database database;

    public IServiceHandler(Database db) {
        super();
        this.database = db;
    }

    @Override
    public GetTimeResp getTime(GetTimeReq req) throws TException {
        GetTimeResp resp = new GetTimeResp();
        resp.setTime(new Date().toString());
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ConnectResp connect(ConnectReq req) throws TException {
        // TODO
        ConnectResp resp = new ConnectResp();
        String username = req.getUsername();
        String password = req.getPassword();
        if (username.equals(Global.USERNAME) && password.equals(Global.PASSWORD)) {
            resp.setStatus(new Status(Global.SUCCESS_CODE));
            long sessionid = (new Random()).nextLong();
            while (sessionid == 0) {
                sessionid = (new Random()).nextLong();
            }
            resp.setSessionId(sessionid);
            connected_sessionid.add(sessionid);
            executerList.put(sessionid, new StatementExecuter(database, sessionid));
        } else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
            resp.setSessionId(0L);
        }
        return resp;
    }

    @Override
    public DisconnetResp disconnect(DisconnetReq req) throws TException {
        // TODO
        DisconnetResp resp = new DisconnetResp();
        long sessionid = req.getSessionId();
        if (connected_sessionid.contains(sessionid)) {
            connected_sessionid.remove(sessionid);
            executerList.remove(sessionid);
            resp.setStatus(new Status(Global.SUCCESS_CODE));
        } else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
        }
        return resp;
    }

    @Override
    public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
        // TODO
        ExecuteStatementResp resp = new ExecuteStatementResp();
        long sessionid = req.getSessionId();
        if (connected_sessionid.contains(sessionid)) {
            String statement = req.getStatement();
            // TODO parse the statement
            System.out.println(statement);
            executerList.get(sessionid).execute(statement);
            resp.setStatus(new Status(Global.SUCCESS_CODE));
        } else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
        }
        return resp;
    }
}
