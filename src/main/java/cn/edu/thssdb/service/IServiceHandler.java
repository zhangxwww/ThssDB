package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import javax.xml.bind.annotation.XmlElementDecl;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class IServiceHandler implements IService.Iface {

    private Set<Long> connected_sessionid = new HashSet<>();
    private StatementExecuter executer = new StatementExecuter();
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
            resp.setSessionId(sessionid);
            connected_sessionid.add(sessionid);
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
            executer.execute(statement);
            resp.setStatus(new Status(Global.SUCCESS_CODE));
        } else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
        }
        return resp;
    }
}
