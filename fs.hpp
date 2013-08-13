// -*-mode:c++; coding:utf-8-*-

#ifndef _FILESYSTEM_HPP_
#define _FILESYSTEM_HPP_

#include <boost/filesystem/path.hpp>
#include <boost/asio/buffer.hpp>

#include "localfs.hpp"
// 向命名空间中加入一些其它便利的操作
namespace localfs
{
#include "fs.ipp"		
}

/*
#include "otherfs.hpp"
namespace otherfs
{
#include "fs.ipp"		
}
*/
	

// 文件系统命名空间别名	
namespace fs = gfs;

#endif
