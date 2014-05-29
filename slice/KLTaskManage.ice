// **********************************************************************
//
// Copyright (c) 2003-2009 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#ifndef KLBD_ICE
#define KLBD_ICE

module KLBD
{
	struct KLTaskInfo
	{
		string taskname;  //任务名称
		string operatorid;//操作员id
		string sysdate;   //任务建立的时间
		long type;        //任务类型
		long status;      //状态 未处理 0 正在运行 1,成功 2,失败 3,kill的 5
		string para;      //该任务所需的参数值，每个任务数据可能不同
	};
	sequence<KLTaskInfo> KLTaskArr;
	
	
	interface KLBDTaskMg
	{
		//data 该任务所需的参数值，每个任务数据可能不同
		//taskname  任务名称，每个任务名称只能使用一次
		//tasktype  任务类型
		//return 0 succ -1 任务名称不能有.号 -2 任务名称重复
		long AddTask(string data, string taskname, string operatorid, long tasktype);

		//jobname  任务名称
		//status  状态 未处理 0 正在运行 1,成功 2,失败 3,kill的 5
		long GetJobStatus(string jobname, out long status);

		//begindate 开始时间  yyyymmdd
		//endate  结束时间    yyyymmdd
		long GetJobList(string begindate, string enddate, out KLTaskArr list);
		void shutdown();
	};

};

#endif
