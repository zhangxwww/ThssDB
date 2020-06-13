package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import javax.xml.bind.annotation.XmlElementDecl;
import java.util.*;

public class IServiceHandler implements IService.Iface {

    private Set<Long> connected_sessionid = new HashSet<>();
    private HashMap<Long, StatementExecuter> executerList = new HashMap<>();
    private Database database;
    private Manager manager;

    public IServiceHandler(Manager manager) {
        super();
        this.database = null;
        this.manager = manager;
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
            executerList.put(sessionid, new StatementExecuter(manager, sessionid));
        } else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
            resp.setSessionId(0L);
        }
        return resp;
    }

    @Override
    public DisconnectResp disconnect(DisconnectReq req) throws TException {
        // TODO
        DisconnectResp resp = new DisconnectResp();
        long sessionid = req.getSessionId();
        if (connected_sessionid.contains(sessionid)) {
            connected_sessionid.remove(sessionid);
            StatementExecuter executer = executerList.get(sessionid);
            executer.disconnect();
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

            int code;
            boolean isAbort = true;
            boolean hasResult = false;
            // TODO hasResult, lists...
            try {
                StatementExecuter executer = executerList.get(sessionid);
                executer.execute(statement);
                code = Global.SUCCESS_CODE;
                isAbort = false;
                List<String> columnList = new ArrayList<>();
                List<List<String>> rowList = new ArrayList<>();
                if (executer.getResult(columnList, rowList)) {
                    hasResult = true;
                    resp.setColumnsList(columnList);
                    resp.setRowList(rowList);
                }
            } catch (AmbiguousColumnException e) {
                code = e.code();
            } catch (AttrNotExistsException e) {
                code = e.code();
            } catch (PrimaryKeyRequiredException e) {
                code = e.code();
            } catch (TableNotExistsException e) {
                code = e.code();
            } catch (WrongInsertArgumentNumException e) {
                code = e.code();
            } catch (DuplicateKeyException e) {
                code = e.code();
            } catch (KeyNotExistException e) {
                code = e.code();
            } catch (DuplicateTableNameException e) {
                code = e.code();
            } catch (StringValueExceedLengthException e) {
                code = e.code();
            } catch (ColumnTypeWrongException e) {
                code = e.code();
            } catch (NotNullAttributeAssignedNullException e) {
                code = e.code();
            } catch (SyntaxErrorException e) {
                code = e.code();
            }
            resp.setStatus(new Status(code));
            resp.setIsAbort(isAbort);
            resp.setHasResult(hasResult);
        } else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
        }
        return resp;
    }
}
