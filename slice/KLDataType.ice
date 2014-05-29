// **********************************************************************
//
// Copyright (c) 2003-2013 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************

#pragma once

module KLTypeModule
{
	// 数据类型
	sequence<bool> BoolSeq;
	sequence<long> LongSeq;
	sequence<string> StrSeq;
	dictionary<string, StrSeq> KLDataMap;
};

