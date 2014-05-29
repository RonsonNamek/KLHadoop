package IceServer;


//**********************************************************************
//
//Copyright (c) 2003-2013 ZeroC, Inc. All rights reserved.
//
//This copy of Ice is licensed to you under the terms described in the
//ICE_LICENSE file included in this distribution.
//
//**********************************************************************



import Ice.Current;
import KLBD.*;
import KLInterfaceModule.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;

import org.apache.log4j.Logger;

import sms_data.Sms;
import sms_data.SmsDao;
import sms_data.SmsSearch;




public class KLHadoopI extends _KLInterfaceDisp
{

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(KLHadoopI.class);

	
	public KLHadoopI(String name)
    {
		_name = name;
        System.out.println(_name + ": starting...");
    }


	@Override
	public void
	klInvoke_async(AMD_KLInterface_klInvoke __cb, String operation, Map<String, String[]> indata, Current __current)
	{
		logger.info("Begin klInvoke.");
		//定义输出参数
		Map<String, String[]> outdata = new HashMap<String, String[]>();
		//返回值
		long stataus = 0;

		if(operation.charAt(2) == '1')
		{
			//任务管理器
			KLBDTaskMgPrx klmt = null;
			try
	        {
				klmt = KLBDTaskMgPrxHelper.uncheckedCast(__current.adapter.getCommunicator().propertyToProxy("KLTaskManager.Proxy"));
				if(klmt != null)
				{
					// TODO
					stataus = -1;
				}
				klmt.ice_ping();
		        logger.info("Task ping success.");
	        }
	        catch(Ice.LocalException ex)
	        {
	            ex.printStackTrace();
	            System.err.println(ex);
	            stataus = -1;
	        }
			
			
			if(operation.equals("111007"))		//创建热点词频分析任务的解析
			{
	            String vparams = null;
	            String voperatorid = null;
	            long ntasktype = 0;
	            String vtaskname = null;

	            //定义迭代器
	            Set<Entry<String, String[]>> set = indata.entrySet();
	    		Iterator<Entry<String, String[]>> iterator = set.iterator();
	            while (iterator.hasNext())
	            {

	            	Entry<String, String[]> mapentry = iterator.next();
	            	String keyname = mapentry.getKey();
	                if(keyname.equals("vparams"))
	                {
	                	vparams = mapentry.getValue()[0];
	                }
	                else if(keyname.equals("voperatorid"))
	                {
	                	voperatorid = mapentry.getValue()[0];       
	                }
	                else if(keyname.equals("ntasktype"))
	                {
	                	ntasktype = Long.parseLong(mapentry.getValue()[0]);               
	                }
	                else if(keyname.equals("vtaskname"))
	                {
	                	vtaskname = mapentry.getValue()[0];               
	                }
	            }
	            try
	            {
	                klmt.AddTask(vparams, vtaskname, voperatorid, ntasktype);
	            }
	            catch(Ice.LocalException ex)
	            {
	                ex.printStackTrace();
	                System.err.println(ex);
	            }
		    }
			else if (operation.equals("111008"))	//获取所有任务的列表的解析
			{
		        String vbegindate = null;
		        String venddate = null;
		        Set<Entry<String, String[]>> set = indata.entrySet();
	    		Iterator<Entry<String, String[]>> iterator = set.iterator();
	            while (iterator.hasNext())
	            {
	            	Entry<String, String[]> mapentry = iterator.next();
	            	String keyname = mapentry.getKey();
	                if(keyname.equals("vbegindate"))
	                {
	                    vbegindate = mapentry.getValue()[0];
	                }
	                else if(keyname.equals("venddate"))
	                {
	                    venddate = mapentry.getValue()[0];               
	                }
	            }
	            
	            KLTaskArrHolder tasklist = new KLTaskArrHolder();
	            try
	            {
	                klmt.GetJobList(vbegindate, venddate, tasklist);
	            }
	            catch(Ice.LocalException ex)
	            {
	                ex.printStackTrace();
	                System.err.println(ex);
	            }
	            
	            int size = tasklist.value.length;	            
	            String[] taskname = new String[size];
	            String[] sysdate = new String[size];
	            String[] type = new String[size];
	            String[] status = new String[size];
	            String[] params = new String[size];
	            String[] operationid = new String[size];
	            for (int i = 0; i < size; i++)
	            {
	            	KLTaskInfo info = tasklist.value[i];
	            	taskname[i] = info.taskname;
	            	sysdate[i] = info.sysdate;
	            	type[i] = Long.toString(info.type);
	            	status[i] = Long.toString(info.status);
	            	params[i] = info.para;
	            	operationid[i] = info.operatorid;
	            }
		        outdata.put("vtaskname", taskname);
		        outdata.put("vcreatedate", sysdate);
		        outdata.put("ntasktype", type);
		        outdata.put("ntaskstatus", status);
		        outdata.put("vparams", params);
		        outdata.put("voperationid", params);
			}
		}
		else if (operation.charAt(2) == '0')
		{
			// 查询短信接口
			List<Sms> smslist = new LinkedList<Sms>();
			if (operation.equals("120003"))	//组合条件查询接口的解析
			{
				SmsDao test1 = new SmsDao();
				try
				{
					smslist = test1.getSms(indata);
				}
				catch (Exception ex)
				{
					java.util.logging.Logger.getLogger(KLHadoopI.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
			else if (operation.equals("120009"))	//特征值查询接口的解析
			{				
				SmsSearch test1 = new SmsSearch();
				try
				{
					smslist = test1.getSms(indata);
				}
				catch (Exception ex)
				{
					java.util.logging.Logger.getLogger(KLHadoopI.class.getName()).log(Level.SEVERE, null, ex);
				}
			}

	        List<String> vsrcnum_array = new LinkedList<String>();
	        List<String> vsrccity_array = new LinkedList<String>();
	        List<String> vdesnum_array = new LinkedList<String>();
	        List<String> vdescity_array = new LinkedList<String>();
	        List<String> vsendtime_array = new LinkedList<String>();
	        List<String> vsmscontent_array = new LinkedList<String>();
	        for(int i=0; i < smslist.size(); i++)
	        {
	            vsrcnum_array.add(smslist.get(i).getSendnum());
	            vsrccity_array.add(smslist.get(i).getSendlocation());
	            vdesnum_array.add(smslist.get(i).getReceivenum());
	            vdescity_array.add(smslist.get(i).getReceivelocation());
	            vsendtime_array.add(smslist.get(i).getSendtime());
	            vsmscontent_array.add(smslist.get(i).getContent());
	        }
	        outdata.put("vsrcnum",  vsrcnum_array.toArray(new String[vsrcnum_array.size()]));
	        outdata.put("vsrccityid",  vsrccity_array.toArray(new String[vsrccity_array.size()]));
	        outdata.put("vdesnum",  vdesnum_array.toArray(new String[vdesnum_array.size()]));
	        outdata.put("vdescityid",  vdescity_array.toArray(new String[vdescity_array.size()]));
	        outdata.put("vsendtime",  vsendtime_array.toArray(new String[vsendtime_array.size()]));
	        outdata.put("vsmscontent",  vsmscontent_array.toArray(new String[vsmscontent_array.size()]));
		}

        logger.info("Finish klInvoke.");
        //
        __cb.ice_response(stataus, outdata);
	}

    private String _name;

}

