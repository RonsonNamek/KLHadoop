package IceServer;

// **********************************************************************
//
// Copyright (c) 2003-2013 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************



//public class KLTaskManagerSvr extends Ice.Application
public class KLHadoopSvc extends Ice.Application
{
	 public int
	 run(String[] args)
	 {		 
	     Ice.ObjectAdapter adapter = communicator().createObjectAdapter("KLHadoop");
	     Ice.Properties properties = communicator().getProperties();
	     Ice.Identity id = communicator().stringToIdentity(properties.getProperty("KLHadoopSrv.Identity"));
	     
	     adapter.add(new KLHadoopI(properties.getProperty("Ice.ProgramName")), id);
	     adapter.activate();
	     communicator().waitForShutdown();
	     return 0;
	 }

	 static public void
	 main(String[] args)
	 {
		 KLHadoopSvc app = new KLHadoopSvc();
	     int status = app.main(args[0], args);
	     System.exit(status);
	 }
}