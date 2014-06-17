// **********************************************************************
//
// Copyright (c) 2003-2013 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

#include "KLDataType.ice"

module KLInterfaceModule
{
	// DataCenter
	interface KLInterface
	{		
		["amd"] long klInvoke(string operation, KLTypeModule::KLDataMap indata, out KLTypeModule::KLDataMap outdata);
	};
	sequence<KLInterface*> KLInterfacePrxList;
};
